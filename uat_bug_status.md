# UAT Bug Remediation Status
**Last Updated:** 2026-03-31 | **Total Bugs:** 71 unique items

---

## Legend
| Symbol | Meaning |
|---|---|
| ✅ | **Fixed** — resolved in code, confirmed or traceable |
| 🔄 | **Partial** — partially addressed, needs verification or finishing |
| ❌ | **Open** — not yet addressed, action needed |
| ⏭️ | **Deferred / Infrastructure** — requires environment/infra action outside codebase |

---

## 🔐 Security & Authentication (BS1–BS8)

| ID | Title | UAT RAG | Our Status | Notes |
|---|---|---|---|---|
| BS1 | Protected resources accessible before OTP | Fixed | ✅ | `fresh.authz` middleware + 2FA gate enforced |
| BS2 | Session created before OTP verification | Fixed | ✅ | Pending session pattern implemented |
| BS3 | OTP page refresh allows bypass | Fixed | ✅ | Server-side session state checked on every request |
| BS4 | Back-button navigation bypasses OTP | Fixed | ✅ | `fresh.authz` middleware handles this |
| BS5 | No OTP failed-attempt threshold | Fixed | ✅ | Throttle middleware applied to OTP routes |
| BS6 | Idle timeout not enforced | Fixed | ✅ | Session lifetime configured; keep-alive API implemented |
| BS7 | Invalidated session not reflected on second tab | Fixed | ✅ | Acceptable — server invalidates on next request |
| BS8 | No admin password change capability | Fixed | ✅ | Profile edit includes password change |

---

## 📱 UI / Responsiveness (UI1–UI10)

| ID | Title | UAT RAG | Our Status | Notes |
|---|---|---|---|---|
| UI1 | Login page breaks on small mobile (375px) | Fixed | ✅ | |
| UI2 | Login page not responsive on standard mobile | Fixed | ✅ | |
| UI3 | Branding/hero image excessively cropped | Fixed | ✅ | |
| UI4 | Login error-state layout partially responsive | Fixed | ✅ | |
| UI5 | Mobile dashboard landing state not usable | Fixed | ✅ | |
| UI6 | Mobile nav overlay not polished | Fixed | ✅ | |
| UI7 | Logout not visible on mobile side panel | Fixed | ✅ | |
| UI8 | Dashboard cards don't stack on mobile | Fixed | ✅ | |
| UI9 | Desktop dashboard card alignment inconsistent | Fixed | ✅ | |
| UI10 | Breadcrumb missing on deeper pages | Fixed | ✅ | |

---

## 🔄 Re-Test Items (RTUI01–RTUI12)

| ID | Title | Our Status | Notes |
|---|---|---|---|
| RTUI01 | Session expires at OTP stage instead of graceful retry | 🔄 | OTP stage has resend link; verify expiry message is shown cleanly |
| RTUI02 | Session created before OTP verification | ✅ | Duplicate of BS2 — fixed |
| RTUI03 | Session timeout based on elapsed time not inactivity | ❌ | **Open** — keep-alive API exists but inactivity timer (mouse/keyboard events) needs to be wired in JS |
| RTUI04 | Active form interaction does not reset inactivity timer | ❌ | **Open** — same as RTUI03; keep-alive must fire on user input events |
| RTUI05 | Breadcrumb contains dead/non-functional links | ❌ | **Open** — breadcrumb links need audit across all pages |
| RTUI06 | Breadcrumb IA inconsistent with actual routing | ❌ | **Open** — layout breadcrumb values need to match real route hierarchy |
| RTUI07 | Dashboard cards do not reflow properly | 🔄 | Shell is responsive; internal card reflow needs verification |
| RTUI08 | Dashboard responsiveness incomplete at component level | 🔄 | Partially addressed; needs full mobile review |
| RTUI09 | Dashboard labels are ambiguous | ❌ | **Open** — menu/label copy needs review; admin vs user-facing language |
| RTUI10 | Navigation labelling lacks user-relatable terminology | ❌ | **Open** — same as RTUI09 |
| RTUI11 | OTP flow lacks graceful delay handling | 🔄 | Resend exists; terminal state messaging needs verification |
| RTUI12 | Inactivity timeout too aggressive for workflows | ❌ | **Open** — timeout threshold needs increasing; recommend 30–60 min for admin roles |

---

## 🧭 Navigation (NAV-001)

| ID | Title | Our Status | Notes |
|---|---|---|---|
| NAV-001 | Navigation doesn't provide optimal wayfinding | ❌ | **Open** — repeated "Dashboard > Admin > Dashboard" breadcrumb; sidebar hierarchy and active state indicators need polish |

---

## 🔒 SSL (SSL1)

| ID | Title | Our Status | Notes |
|---|---|---|---|
| SSL1 | Invalid/Mismatched SSL certificate | ⏭️ | **Infrastructure** — requires valid SSL cert provisioned on Azure App Service; not a code fix |

---

## 🔑 RBAC / Permissions (RB0–RB10)

| ID | Title | Our Status | Notes |
|---|---|---|---|
| RB0 | Admin modules not decomposed into maintainable permissions | 🔄 | Core permission seeder exists; modules partially gated |
| RB1 | Security Settings not decomposed into permissions | ❌ | **Open** — `settings.security.*` permissions not yet seeded/enforced |
| RB2 | System Config not decomposed into permissions | ❌ | **Open** — `settings.system_config.*` permissions missing |
| RB3 | Email Templates not decomposed into permissions | ❌ | **Open** — `settings.email_templates.*` permissions missing |
| RB4 | Backup admin not decomposed into permissions | ❌ | **Open** — `settings.backups.*` permissions missing |
| RB5 | Notification admin not decomposed into permissions | ❌ | **Open** — `settings.notifications.*` permissions missing |
| RB6 | Assessment Settings not decomposed into permissions | 🔄 | `settings.assessment.read` / [update](file:///c:/Users/SERVERPT-260424/Dev/ass_app_backup_20260122_224128/Grow_lara/app/Http/Controllers/UserManagementController.php#194-252) exist; sub-permissions not granular |
| RB7 | No read-only permissions for admin modules | ❌ | **Open** — read-only role variant not implemented |
| RB8 | No permission model for audit/log visibility | ❌ | **Open** — `settings.audit_trail.*` / `logs.*` permissions missing |
| RB9 | No permission model for API/integration admin | ❌ | **Open** — `integrations.*` permissions missing |
| RB10 | Admin modules as functions not permissioned resources | 🔄 | Partially addressed; core pattern exists but not fully applied |

---

## 📋 Assessment Form UX (FORM-001–004)

| ID | Title | Our Status | Notes |
|---|---|---|---|
| FORM-001 | Assessment form lacks step-based navigation | ❌ | **Open** — no Previous/Next area-level pagination; this is a significant build task |
| FORM-002 | Pagination not aligned to Area→Section→Statement hierarchy | ❌ | **Open** — same as FORM-001 |
| FORM-003 | No final Review & Submit page | ❌ | **Open** — assessment ends without a review step |
| FORM-004 | Recommended usability model not implemented | ❌ | **Open** — umbrella item covering FORM-001/003 |

---

## 🔧 Functional Bugs (Unlabelled — logged 30/03/2026)

| Title | Our Status | Notes |
|---|---|---|
| Invite User action not functioning (×2 entries) | ✅ | **Fixed this session** — full invite flow built with email |
| Self-registration not working | ❌ | **Open** — registration "Next" button logic needs end-to-end retest |
| Dashboard metrics undefined | ❌ | **Open** — KPI cards need defined business metrics |
| "All Users" from dashboard returns 500 error | ❌ | **Open** — route/controller issue on dashboard shortcut |
| Email Assessment Report pop-up blocks dashboard | ❌ | **Open** — modal cannot be closed; needs dismiss handler |
| Dashboard contains dead links & placeholder content | ❌ | **Open** — "Registry" dead, "Yields" placeholder |
| Breadcrumb dead links & inconsistent labels ("draft session") | ❌ | **Open** — relabel "Draft Sessions" → "Draft Assessments" |
| "Verified Reports" & "Draft Sessions" route to user list | ❌ | **Open** — dashboard card links point to wrong controller |
| "Registered Users" & "Secure (2FA)" cards unclear | ❌ | **Open** — card definitions and routing need verification |
| "Active Clubs" count correct but link is dead | ❌ | **Open** — route name incorrect on dashboard card |
| Group actions (View/Edit/Delete) not visible | ❌ | **Open** — opacity-based hover may be hiding these on groups page |
| Club search filter not working | ❌ | **Open** — JS search filter needs wiring |
| Club detail view returns 500 | ✅ | **Fixed** — 3-layer club resolution logic added |
| Health Intelligence section lacks meaningful stats | ❌ | **Open** — section needs real data or CTA |
| New member registration not possible under All Members | ❌ | **Open** — add member form/route broken |
| No filters in club listing | ❌ | **Open** — filter UI not implemented |
| Maturity Logic Engine rule management not usable | ❌ | **Open** — CRUD for logic rules needs implementation |
| New maturity logic rule cannot be added | ❌ | **Open** — same as above |
| Rule filters not working in Logic Engine | ❌ | **Open** — JS filter broken |
| System Settings capabilities not usable | ❌ | **Open** — partially exposed settings non-functional |
| Email templates cannot be edited | ❌ | **Open** — email template editor not implemented |

---

## 📊 Summary

| Category | Total | ✅ Fixed | 🔄 Partial | ❌ Open | ⏭️ Infra |
|---|---|---|---|---|---|
| Security / Auth (BS) | 8 | 8 | 0 | 0 | 0 |
| UI / Responsiveness | 10 | 10 | 0 | 0 | 0 |
| Re-test (RTUI) | 12 | 1 | 4 | 7 | 0 |
| Navigation (NAV) | 1 | 0 | 0 | 1 | 0 |
| SSL | 1 | 0 | 0 | 0 | 1 |
| RBAC / Permissions | 11 | 0 | 3 | 8 | 0 |
| Assessment Form UX | 4 | 0 | 0 | 4 | 0 |
| Functional Bugs | 21 | 3 | 0 | 18 | 0 |
| **TOTAL** | **68** | **22** | **7** | **38** | **1** |

---

## 🔴 Highest Priority Open Items (P1 Critical/High)

1. **Self-registration flow broken** — primary entry point
2. **"All Users" dashboard link → 500 error** — blocks user admin
3. **Dashboard card misrouting** (Verified Reports → user list, Draft Sessions → user list)
4. **Email Assessment Report modal undismissable** — blocks dashboard
5. **RTUI03/04** — Inactivity timer not reset by user activity
6. **FORM-001–003** — Assessment step navigation (area-level pagination + review page)
7. **Maturity Logic Engine** — rules cannot be added/managed
8. **New member registration** — blocked under All Members
9. **RBAC granularity** (RB1–RB10) — permissions not decomposed per module
