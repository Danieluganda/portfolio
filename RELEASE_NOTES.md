# Portfolio Dashboard Release Notes

## 2026-06-06

### Data Sync
- Added `sync_data.bat` to regenerate `data.js` and write timestamped logs.
- Added `setup_scheduled_sync.bat` with a default 3-times-daily Kobo sync schedule: 07:00, 12:00, and 17:00.
- Added `remove_scheduled_sync.bat` to remove the Windows scheduled task.
- Confirmed the scheduled task can run successfully through Windows Task Scheduler.

### Dashboard
- Renamed the Intelligence section to Analysis.
- Fixed period filters so they read the actual raw date field (`d`) and common date variants.
- Verified period filters produce different counts for Today, Yesterday, Week, Month, and Quarter.
- Added browser auto-refresh shortly after scheduled sync windows so users are less likely to view stale `data.js`.
- Improved responsive layouts and compact KPI cards so summary rows fit better on desktop and collapse on smaller screens.

### Digital Credit
- Reworked the Digital Credit page around demand and screening readiness rather than implying confirmed credit usage.
- Added EOI cross-reference indicators for business registration, National ID evidence, TIN, and NSSF where available.
- Added notes for fields not yet available in source data, including provider approval, disbursement, repayment, and phone-in-owner-name checks.

### Security And Local Setup
- Added `.gitignore` rules for Kobo credentials, Kobo API cache, sync logs, Python caches, and scratch files.
- Added `kobo_config.example.json` as a token-free setup template.
- Updated `extract_data.py` to support `kobo_config.local.json` and the `KOBO_TOKEN` environment variable.

### Known Notes
- Windows scheduled tasks are local machine settings. Any new machine must run `setup_scheduled_sync.bat`.
- The dashboard is still a static frontend. It refreshes itself after scheduled sync windows, but it does not stream live updates.
- Some aggregate-only metrics remain all-time if the source data does not include raw dated records.
