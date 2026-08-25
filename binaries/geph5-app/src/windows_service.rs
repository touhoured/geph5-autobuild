use std::{
    ffi::OsString,
    path::Path,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use anyhow::Context;
use windows_service::{
    define_windows_service,
    service::{
        ServiceAccess, ServiceAction, ServiceActionType, ServiceControl, ServiceControlAccept,
        ServiceErrorControl, ServiceExitCode, ServiceFailureActions, ServiceFailureResetPeriod,
        ServiceInfo, ServiceStartType, ServiceState, ServiceStatus, ServiceType,
    },
    service_control_handler::{self, ServiceControlHandlerResult, ServiceStatusHandle},
    service_dispatcher,
    service_manager::{ServiceManager, ServiceManagerAccess},
};

pub(crate) const SERVICE_NAME: &str = "GephManager";
const DISPLAY_NAME: &str = "Geph Manager";
const DESCRIPTION: &str = "Geph5 privileged network manager";
const STOP_WAIT: Duration = Duration::from_secs(60);

define_windows_service!(ffi_service_main, service_main);

pub(crate) fn run_dispatcher() -> anyhow::Result<()> {
    service_dispatcher::start(SERVICE_NAME, ffi_service_main)
        .context("starting the Geph Manager service dispatcher")
}

fn service_main(_arguments: Vec<OsString>) {
    crate::init_manager_logging();
    if let Err(error) = run_service() {
        tracing::error!(error = %format!("{error:#}"), "Geph Manager service failed");
    }
}

fn status(
    state: ServiceState,
    controls: ServiceControlAccept,
    exit_code: ServiceExitCode,
    checkpoint: u32,
    wait_hint: Duration,
) -> ServiceStatus {
    ServiceStatus {
        service_type: ServiceType::OWN_PROCESS,
        current_state: state,
        controls_accepted: controls,
        exit_code,
        checkpoint,
        wait_hint,
        process_id: None,
    }
}

fn set_running(handle: &ServiceStatusHandle) -> windows_service::Result<()> {
    handle.set_service_status(status(
        ServiceState::Running,
        ServiceControlAccept::STOP | ServiceControlAccept::PRESHUTDOWN,
        ServiceExitCode::NO_ERROR,
        0,
        Duration::ZERO,
    ))
}

fn run_service() -> anyhow::Result<()> {
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let shutdown_tx = Arc::new(Mutex::new(Some(shutdown_tx)));
    let handler_tx = shutdown_tx.clone();
    let event_handler = move |control| match control {
        ServiceControl::Stop | ServiceControl::Preshutdown => {
            if let Some(sender) = handler_tx.lock().expect("shutdown sender poisoned").take() {
                let _ = sender.send(());
            }
            ServiceControlHandlerResult::NoError
        }
        ServiceControl::Interrogate => ServiceControlHandlerResult::NoError,
        _ => ServiceControlHandlerResult::NotImplemented,
    };
    let status_handle = service_control_handler::register(SERVICE_NAME, event_handler)
        .context("registering the service control handler")?;
    status_handle.set_service_status(status(
        ServiceState::StartPending,
        ServiceControlAccept::empty(),
        ServiceExitCode::NO_ERROR,
        1,
        Duration::from_secs(30),
    ))?;

    let stop_handle = status_handle;
    let shutdown = async move {
        let _ = shutdown_rx.await;
        if let Err(error) = stop_handle.set_service_status(status(
            ServiceState::StopPending,
            ServiceControlAccept::empty(),
            ServiceExitCode::NO_ERROR,
            1,
            STOP_WAIT,
        )) {
            tracing::warn!(%error, "could not report service stop-pending status");
        }
    };
    let ready_handle = status_handle;
    let result = geph5_rt::block_on(crate::manager::run_manager(shutdown, move || {
        if let Err(error) = set_running(&ready_handle) {
            tracing::error!(%error, "could not report running service status");
        }
    }));

    let exit_code = if result.is_ok() {
        ServiceExitCode::NO_ERROR
    } else {
        ServiceExitCode::ServiceSpecific(1)
    };
    status_handle
        .set_service_status(status(
            ServiceState::Stopped,
            ServiceControlAccept::empty(),
            exit_code,
            0,
            Duration::ZERO,
        ))
        .context("reporting stopped service status")?;
    result
}

fn service_info(executable: &Path) -> ServiceInfo {
    ServiceInfo {
        name: OsString::from(SERVICE_NAME),
        display_name: OsString::from(DISPLAY_NAME),
        service_type: ServiceType::OWN_PROCESS,
        start_type: ServiceStartType::AutoStart,
        error_control: ServiceErrorControl::Normal,
        executable_path: executable.to_path_buf(),
        launch_arguments: vec![OsString::from("__manager-service")],
        dependencies: vec![],
        account_name: None,
        account_password: None,
    }
}

pub(crate) fn register_and_start(executable: &Path) -> anyhow::Result<()> {
    let manager = ServiceManager::local_computer(
        None::<&str>,
        ServiceManagerAccess::CONNECT | ServiceManagerAccess::CREATE_SERVICE,
    )
    .context("opening Windows Service Control Manager")?;

    match manager.open_service(SERVICE_NAME, ServiceAccess::ALL_ACCESS) {
        Ok(existing) => {
            stop_service(&existing)?;
            existing
                .delete()
                .context("deleting the previous Geph Manager service")?;
            drop(existing);
            wait_until_deleted(&manager)?;
        }
        Err(error) if win32_error_code(&error) == Some(1060) => {}
        Err(error) => return Err(error).context("opening the previous Geph Manager service"),
    }

    let service = manager
        .create_service(&service_info(executable), ServiceAccess::ALL_ACCESS)
        .context("creating the Geph Manager service")?;
    let configure_and_start = || -> anyhow::Result<()> {
        service.set_description(DESCRIPTION)?;
        service.update_failure_actions(ServiceFailureActions {
            reset_period: ServiceFailureResetPeriod::After(Duration::from_secs(24 * 60 * 60)),
            reboot_msg: None,
            command: None,
            actions: Some(vec![
                ServiceAction {
                    action_type: ServiceActionType::Restart,
                    delay: Duration::from_secs(2),
                },
                ServiceAction {
                    action_type: ServiceActionType::Restart,
                    delay: Duration::from_secs(10),
                },
                ServiceAction {
                    action_type: ServiceActionType::Restart,
                    delay: Duration::from_secs(30),
                },
            ]),
        })?;
        service.set_failure_actions_on_non_crash_failures(true)?;
        service.set_preshutdown_timeout(STOP_WAIT)?;
        service
            .start(&[] as &[&str])
            .context("starting the Geph Manager service")?;
        wait_for_state(&service, ServiceState::Running, Duration::from_secs(45))
            .context("waiting for the Geph Manager service to start")
    };
    if let Err(error) = configure_and_start() {
        let _ = stop_service(&service);
        let _ = service.delete();
        return Err(error);
    }
    Ok(())
}

pub(crate) fn stop_and_delete() -> anyhow::Result<()> {
    let manager = ServiceManager::local_computer(None::<&str>, ServiceManagerAccess::CONNECT)
        .context("opening Windows Service Control Manager")?;
    let service = match manager.open_service(SERVICE_NAME, ServiceAccess::ALL_ACCESS) {
        Ok(service) => service,
        Err(error) if win32_error_code(&error) == Some(1060) => return Ok(()),
        Err(error) => return Err(error).context("opening the Geph Manager service"),
    };
    stop_service(&service)?;
    service
        .delete()
        .context("deleting the Geph Manager service")?;
    Ok(())
}

fn stop_service(service: &windows_service::service::Service) -> anyhow::Result<()> {
    let current = service
        .query_status()
        .context("querying Geph Manager service status")?;
    if current.current_state == ServiceState::Stopped {
        return Ok(());
    }
    if current.current_state != ServiceState::StopPending {
        service
            .stop()
            .context("stopping the Geph Manager service")?;
    }
    wait_for_state(service, ServiceState::Stopped, STOP_WAIT)
        .context("waiting for the Geph Manager service to stop")
}

fn wait_for_state(
    service: &windows_service::service::Service,
    desired: ServiceState,
    timeout: Duration,
) -> anyhow::Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        let current = service.query_status()?;
        if current.current_state == desired {
            return Ok(());
        }
        if Instant::now() >= deadline {
            anyhow::bail!(
                "timed out waiting for service state {desired:?}; current state is {:?}",
                current.current_state
            );
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

fn wait_until_deleted(manager: &ServiceManager) -> anyhow::Result<()> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match manager.open_service(SERVICE_NAME, ServiceAccess::QUERY_STATUS) {
            Err(error) if win32_error_code(&error) == Some(1060) => return Ok(()),
            Err(error) => {
                return Err(error).context("checking removal of Geph Manager service");
            }
            Ok(service) => drop(service),
        }
        if Instant::now() >= deadline {
            anyhow::bail!("timed out waiting for the previous Geph Manager service to be deleted");
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn win32_error_code(error: &windows_service::Error) -> Option<i32> {
    match error {
        windows_service::Error::Winapi(error) => error.raw_os_error(),
        _ => None,
    }
}
