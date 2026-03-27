# Project Changes Overview

This document summarizes the major improvements that were implemented across the Inventory Webapp during the recent update cycle.

It is intended to complement the main `README.md` by focusing on product changes, workflow improvements, security updates, data-integrity improvements, and testing coverage.

## 1. Frontend and Workflow Improvements

### Dashboard Navigation

- The top horizontal tabs were replaced with a vertical sidebar navigation.
- The sidebar now uses icons for each area and expands to reveal labels and session controls.
- Session details, password change, and logout actions were moved into the sidebar.
- The layout now behaves like a dashboard shell instead of a tab strip across the top of the page.
- The dashboard KPI area was later reworked into a denser two-level layout so more operational information fits on screen without wasting vertical space.
- `Pending Productions Due Soon` now includes production comments directly in the dashboard for quick context.
- `Top 10 Missing Parts` replaced the earlier low-stock count summary so shortages are more actionable at a glance.

### Current Stock

- The application now opens with the `Current Stock` view already focused on low-stock items.
- A reorder-oriented export flow was added through `Generate Reorder PDF`.
- The `Add/Remove Parts` area was renamed to `Add/Remove Stock Parts` for clarity.
- The stock table now uses client-side pagination instead of an endless scroll pattern.
- Current Stock rows are now shown in batches of 10 items per page, with page navigation controls and a visible results summary.

### Buildable Product View

- The panel was redesigned to be easier to read visually.
- Long unstructured text was replaced with a cleaner card-based layout.
- The panel now focuses only on the materials used to build the selected product.
- Buildability summary data was removed from this view to avoid duplication with the `Production Planner`.
- Extra details such as stock, Tricoma number, and quantity-per-product were removed from the material cards to keep the panel lightweight.

### Production Planner

- A dedicated `Production Planner` tab was added.
- Users can select a product, enter a target quantity, calculate required material, and review the result in a structured table.
- The planner shows:
  - required quantity
  - current stock
  - remaining quantity after the plan
  - fulfillment status
- A visual summary for `Maximum Buildable` and `Limiting Parts` was added to the planner.
- The planner results table now uses client-side pagination with 10 rows per page.
- The planner supports PDF export for missing materials.

### Production Schedule

- A dedicated `Production Schedule` workflow was added separately from the material calculator.
- Both `Administrator` and `Standard User` can:
  - queue a production
  - set the required quantity
  - assign a due date
  - add optional comments/notes
- Due dates are now validated so past dates cannot be scheduled.
- The schedule uses allocation logic that reserves material virtually by due date, so earlier jobs consume stock before later ones.
- The schedule table now uses client-side pagination with 5 rows per page.
- The planner-style queue was redesigned to look more like a lightweight job board, with a dedicated `Selected Production` inspector and a separate material-detail table.
- Completed productions are no longer shown inside the `Production Schedule` tab.
- Completing a scheduled production now moves it directly into `Finished Products`.

### Yearly Analytics

- Two annual analytics tabs were added:
  - `Most Used Parts`
  - `Most Produced Products`
- Both views support year-based loading so the dashboard can focus on one production year at a time.
- `Most Used Parts` now shows:
  - a `Top 10` summary for the most consumed individual parts in the selected year
  - the full yearly ranked list below it
- `Most Produced Products` shows yearly production totals grouped by product.
- These views provide quick reporting without needing manual spreadsheet work.

### Transactions

- Transactions now show both:
  - manual stock movements
  - material consumption derived from finished product production
- Production-driven consumption appears as `OUT - Production`.
- The transaction table now includes:
  - filter by movement type
  - filter by user
  - filter by produced product
  - date range filter
  - CSV export
  - PDF export
- A dedicated `Produced Product` column was added so production-driven rows are easier to understand.
- The Transactions table now uses client-side pagination with 10 rows per page.
- The Transactions filter toolbar was reworked to wrap responsively instead of forcing the panel width to grow when the sidebar expands.

### Inventory Setup and Admin Split

- The old combined admin area was split into two clearer sections:
  - `Inventory Setup` for parts, products, and BOM management
  - `Admin` for user management
- This makes the operational setup flow easier to understand and keeps user administration separate from product and material maintenance.
- The tab activation flow for `Inventory Setup` was later hardened so the grouped setup panels refresh reliably when the tab is opened.

### Log Book

- A dedicated admin-only `Log Book` tab was added.
- It provides a readable in-app log viewer for:
  - errors
  - warnings
  - critical failures
  - debug and informational events
- The view includes:
  - search
  - level filter
  - date range filter
  - pagination
  - expandable details for structured log payloads
- This gives administrators direct visibility into backend events without needing to inspect container logs manually.

## 2. PDF and Export Features

### Reorder PDF

- A reorder PDF can now be generated from low-stock items in `Current Stock`.
- The report includes:
  - part name
  - current stock
  - reorder level
  - reorder quantity
  - Tricoma number

### Production Planner PDF

- The planner can generate a missing-material PDF after a calculation.
- The report includes:
  - product name
  - quantity to produce
  - missing parts only
  - shortage quantity
  - Tricoma number
  - reorder quantity

### Transactions Export

- Filtered transaction data can now be exported as:
  - CSV
  - PDF
- Exports respect the same filters used in the Transactions tab so downloaded reports match what the user is viewing.

## 3. Authentication, Roles, and Access Control

### Login System

- The application now requires authentication before the dashboard becomes visible.
- A dedicated login overlay was added.

### Roles

Two roles are now supported:

- `Administrator`
  - can access `Current Stock`
  - can access `Finished Products`
  - can access `Production Planner`
  - can access `Transactions`
  - can access `Inventory Setup`
  - can access `Admin`

- `Standard User`
  - can access `Current Stock`
  - can access `Production Planner`
  - can access `Transactions`

Internally, the limited user role still maps to `powermoon`, but the UI now uses clearer labels.

### User Management

- The admin can:
  - create users
  - edit users
  - delete users
- The limited user can:
  - change their own password
- Newly created users are now flagged to change their password on the first successful login before they can use the rest of the app.

## 4. Session Security

- Automatic logout after 5 minutes of inactivity was added on the frontend.
- Cross-tab inactivity synchronization was added so logout state is shared between open tabs.
- Sliding session expiry was also implemented on the backend, so the session now expires server-side after inactivity as well.
- `APP_SESSION_SECRET` is now mandatory so the app cannot start with a predictable fallback session-signing key.
- Login now has server-side rate limiting to reduce brute-force risk.
- Session cookies now support `Secure` behavior when the app runs behind HTTPS.
- CORS is no longer left fully open by default and is instead controlled through an allowlist-oriented configuration.

## 5. Input Validation and Sanitization

- Central backend validation helpers were introduced to make request validation consistent.
- Validation now covers:
  - text normalization
  - numeric parsing
  - ID validation
  - role validation
  - search query sanitization
  - BOM usage payload validation
- Frontend input handling was also tightened so invalid input is blocked earlier in the UI.
- HTML output and PDF output continue to use escaping/sanitization rules before rendering.

## 6. Audit and Traceability

- Transactions now display the user responsible for a movement or production action.
- Finished product creation records who performed the action.
- Manual stock movements continue to support `IN` and `OUT` entries as separate historical records.
- Application-level backend events are now also recorded in `app_logs`, which powers the admin `Log Book` view.

## 7. BOM Snapshot and Historical Data Integrity

One of the most important backend changes was freezing the Bill of Materials at the time of production.

### Problem Solved

Previously, material consumption for finished products was derived dynamically from:

- `finished_products`
- the current BOM

This meant that if the BOM was changed later, historical consumption and transaction history could change retroactively.

### New Approach

Two snapshot tables were introduced:

- `production_runs`
- `production_run_materials`

When a finished product is created:

- a production run record is created
- the current BOM is copied into snapshot rows
- future stock calculations and production transactions use the snapshot instead of the live BOM

### Result

- production history is now stable
- stock history is more reliable
- transaction history no longer changes when the BOM is edited later

Backfill logic was also added so older production records can receive snapshots automatically when possible.

## 8. Finished Product and Stock Behavior

- Creating finished products now reduces material stock through the snapshot-based production history flow.
- Production-related stock consumption appears in `Transactions`.
- Current stock calculations now use snapshot-aware logic instead of the previous live derived approach.
- Manual `Add Finished Products` entry was removed from the UI.
- `Finished Products` now acts as the execution history fed by `Production Schedule` completion.
- Finished product rows were expanded with schedule context such as:
  - schedule ID
  - due date
  - comments
  - created by

## 9. UI Language and Naming Cleanup

Several UI labels were normalized into English for consistency, including:

- `Generate Reorder PDF`
- `Calculate Material`
- `Export PDF`
- `Inventory Setup`
- role labels such as `Administrator` and `Standard User`

## 10. Documentation and Code Readability

- Core frontend and backend files were commented more thoroughly to make the codebase easier to understand for new contributors.
- The main `README.md` was expanded with testing instructions and operational notes.
- `PROJECT_CHANGES.md` was maintained as a running English summary of the delivered feature set.
- Additional cleanup removed stale frontend wiring left behind by the old manual finished-product flow and the old completed-schedule history view.

## 11. Deployment and Bootstrap

- Fresh machines no longer require a manual `replace-db.sh` import just to make the app boot.
- A new MariaDB first-boot schema initializer now creates the core inventory tables automatically when the `db_data` volume is empty.
- The app continues to create and evolve auxiliary tables at startup for:
  - authentication
  - app logs
  - production BOM snapshots
- SQL restore remains available, but it is now optional and only needed when restoring real inventory data from an existing backup.

## 12. Automated Tests

Two levels of automated tests were added under `server/tests/`.

### Unit Tests

`npm test`

Covers:

- password hashing and verification
- session token generation
- session expiry behavior
- cookie helper behavior

### API Smoke Tests

`npm run test:smoke`

Covers:

- login, logout, and authenticated session checks
- role permissions for admin vs limited user
- forced password change for newly created users
- stock movement creation
- finished product creation
- production planner calculation
- PDF export endpoints
- CSV export for transactions
- admin-only log access
- end-to-end transaction visibility for production-driven material consumption

Temporary test records are created and cleaned up automatically during the smoke suite.

## 13. Summary

The system has evolved from a basic inventory CRUD application into a more structured operational dashboard with:

- a dashboard-style sidebar navigation
- role-based access control
- session security
- first-login password hardening
- production planning
- yearly analytics
- exportable reports
- improved transaction visibility
- an in-app administrative log viewer
- paginated data views across key operational tables
- frozen BOM-based production history
- automated test coverage

These changes significantly improve usability, auditability, data integrity, and maintainability.
