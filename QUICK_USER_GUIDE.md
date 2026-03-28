# Unified Navman + Geotab Dashboard (Quick User Guide)

## 1) What this dashboard does
- Single web dashboard for:
  - Navman AU safety, speeding, idling
  - Geotab NZ safety, speeding, idling
- Driver scorecard + top idling/speeding insights
- Adjustable thresholds directly on the screen

## 2) First-time setup (5 minutes)
1. Install Node.js 18+ on the PC.
2. Open this folder in PowerShell.
3. This package already includes a live `config.json`.
4. If credentials ever change, update `config.json`.
5. Install dependencies:
   - `npm install`

## 3) Start the dashboard
- Run:
  - `npm start`
- Open browser:
  - `http://localhost:3500`

## 4) Daily use
1. Choose **Data source**: navman, geotab, or both.
2. Set date/time range (UTC).
3. Click one of:
   - **Load Safety (All)**
   - **Load Speeding**
   - **Load Idling**
4. Adjust thresholds:
   - Speed threshold (kph)
   - Idle threshold (minutes)
   - Driver score threshold
5. Click **Update Insights**.

## 5) Advanced area
Use **Advanced Filters, Sessions, and Geotab Login** for:
- Optional vehicle/driver filters
- Navman session set/logoff
- Geotab save/auth/logoff

## 6) Troubleshooting
- "Failed to fetch":
  - confirm `node server.js` (or `npm start`) is still running
  - refresh browser (`Ctrl+F5`)
- Navman timeout on very large date ranges:
  - reduce date range and load in chunks (e.g., 7 days at a time)
- No rows:
  - confirm credentials in `config.json`
  - confirm date range has activity

## 7) Security note
- Do not email real credentials in `config.json`.
- Share `config.example.json` with blank values for customer installs.
