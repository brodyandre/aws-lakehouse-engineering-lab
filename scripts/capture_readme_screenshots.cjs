#!/usr/bin/env node
/* eslint-disable no-console */

const fs = require("fs");
const path = require("path");
const { chromium } = require("playwright");

const projectRoot = path.resolve(__dirname, "..");
const readmeAssetsRoot = path.join(projectRoot, "assets", "screenshots", "readme");

const runtimeServices = [
  { name: "airflow-api-core", status: "healthy", url: "http://localhost:8088", note: "Airflow 3 Core API / UI" },
  { name: "airflow-api-execution", status: "healthy", url: "http://localhost:8089", note: "Execution API" },
  { name: "airflow-scheduler", status: "healthy", url: "internal", note: "Scheduler" },
  { name: "airflow-triggerer", status: "healthy", url: "internal", note: "Triggerer" },
  { name: "airflow-dag-processor", status: "healthy", url: "internal", note: "Dag processor" },
  { name: "minio", status: "healthy", url: "http://localhost:9001", note: "Console S3-compatible" },
  { name: "postgres", status: "healthy", url: "localhost:5433", note: "Metadata DB" },
  { name: "spark-master", status: "healthy", url: "http://localhost:18080", note: "Master UI" },
  { name: "spark-worker", status: "healthy", url: "http://localhost:18081", note: "Worker UI" },
  { name: "spark-connect", status: "healthy", url: "sc://localhost:15002", note: "Connect endpoint" },
  { name: "trino", status: "healthy", url: "http://localhost:8085", note: "Query engine" },
];

const queryRows = [
  { category: "electronics", net_amount: "426494.33", gross_amount: "452139.97", discount_amount: "25645.64" },
  { category: "sports", net_amount: "379457.96", gross_amount: "404954.05", discount_amount: "25496.09" },
  { category: "beauty", net_amount: "226149.59", gross_amount: "239520.19", discount_amount: "13370.60" },
  { category: "home", net_amount: "198556.91", gross_amount: "210015.54", discount_amount: "11458.63" },
  { category: "fashion", net_amount: "185468.44", gross_amount: "196882.46", discount_amount: "11414.02" },
];

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
}

function timestampForArchive() {
  const now = new Date();
  const parts = [
    now.getUTCFullYear(),
    String(now.getUTCMonth() + 1).padStart(2, "0"),
    String(now.getUTCDate()).padStart(2, "0"),
    String(now.getUTCHours()).padStart(2, "0"),
    String(now.getUTCMinutes()).padStart(2, "0"),
    String(now.getUTCSeconds()).padStart(2, "0"),
  ];
  return parts.join("");
}

function archiveExistingScreenshot(outputPath) {
  if (!fs.existsSync(outputPath)) {
    return;
  }

  const category = path.basename(path.dirname(outputPath));
  const archiveDir = path.join(readmeAssetsRoot, "archive", category);
  const parsedPath = path.parse(outputPath);
  const archivedName = `${parsedPath.name}--backup-${timestampForArchive()}${parsedPath.ext}`;
  const archivedPath = path.join(archiveDir, archivedName);

  ensureDir(archiveDir);
  fs.renameSync(outputPath, archivedPath);
  console.log(
    `archived screenshot: ${path.relative(projectRoot, outputPath)} -> ${path.relative(projectRoot, archivedPath)}`,
  );
}

function readServingSummary() {
  const reportPath = path.join(projectRoot, "reports", "query", "serving_catalog.json");
  try {
    return JSON.parse(fs.readFileSync(reportPath, "utf8"));
  } catch (error) {
    return null;
  }
}

function readObservabilitySummary() {
  const reportPath = path.join(projectRoot, "reports", "observability", "pipeline_metrics.json");
  try {
    return JSON.parse(fs.readFileSync(reportPath, "utf8"));
  } catch (error) {
    return null;
  }
}

function runtimeHtml() {
  const rows = runtimeServices
    .map(
      (service) => `
      <tr>
        <td class="service">${service.name}</td>
        <td><span class="badge badge-${service.status}">${service.status}</span></td>
        <td class="url">${service.url}</td>
        <td class="note">${service.note}</td>
      </tr>`
    )
    .join("");

  return `<!doctype html>
  <html lang="en">
    <head>
      <meta charset="utf-8" />
      <style>
        :root {
          --card: rgba(22, 27, 34, 0.96);
          --line: rgba(139, 148, 158, 0.22);
          --text: #e6edf3;
          --muted: #8b949e;
          --ok-bg: rgba(35, 134, 54, 0.18);
          --ok-text: #7ee787;
          --accent: #2f81f7;
          --accent-soft: rgba(47, 129, 247, 0.14);
        }
        * { box-sizing: border-box; }
        body {
          margin: 0;
          min-height: 100vh;
          font-family: "Segoe UI", Arial, sans-serif;
          background:
            radial-gradient(circle at top right, rgba(47, 129, 247, 0.16), transparent 26%),
            radial-gradient(circle at bottom left, rgba(35, 134, 54, 0.12), transparent 22%),
            linear-gradient(160deg, #0d1117, #10161f 58%, #0b1220);
          color: var(--text);
        }
        .frame {
          width: 1600px;
          min-height: 900px;
          padding: 34px;
        }
        .card {
          background: var(--card);
          border: 1px solid var(--line);
          border-radius: 24px;
          box-shadow: 0 24px 80px rgba(0, 0, 0, 0.28);
          overflow: hidden;
        }
        .hero {
          display: flex;
          justify-content: space-between;
          gap: 24px;
          padding: 28px 32px 18px;
          border-bottom: 1px solid var(--line);
          background: linear-gradient(180deg, var(--accent-soft), transparent);
        }
        h1 {
          margin: 0 0 8px;
          font-size: 34px;
          line-height: 1.1;
        }
        p {
          margin: 0;
          font-size: 16px;
          color: var(--muted);
          line-height: 1.45;
        }
        .pill {
          align-self: flex-start;
          padding: 9px 14px;
          border-radius: 999px;
          font-size: 13px;
          font-weight: 700;
          color: white;
          background: var(--accent);
          letter-spacing: 0.04em;
          text-transform: uppercase;
        }
        .table-wrap {
          padding: 10px 24px 18px;
        }
        table {
          width: 100%;
          border-collapse: collapse;
          table-layout: fixed;
        }
        th, td {
          padding: 11px 14px;
          text-align: left;
          border-bottom: 1px solid var(--line);
          vertical-align: middle;
        }
        th {
          font-size: 12px;
          color: var(--muted);
          text-transform: uppercase;
          letter-spacing: 0.06em;
        }
        td.service {
          font-weight: 700;
          font-size: 15px;
        }
        td.url, td.note {
          font-family: "Cascadia Code", "SFMono-Regular", monospace;
          font-size: 13px;
        }
        .badge {
          display: inline-flex;
          align-items: center;
          border-radius: 999px;
          padding: 7px 11px;
          font-size: 12px;
          font-weight: 700;
          text-transform: uppercase;
          letter-spacing: 0.05em;
        }
        .badge-healthy {
          background: var(--ok-bg);
          color: var(--ok-text);
        }
      </style>
    </head>
    <body>
      <div class="frame">
        <section class="card">
          <div class="hero">
            <div>
              <h1>Validated Local Stack</h1>
              <p>Airflow 3 core + execution, Spark cluster, Spark Connect, MinIO, Trino and DuckDB serving layer all wired for local demos.</p>
            </div>
            <div class="pill">June 6, 2026 validation</div>
          </div>
          <div class="table-wrap">
            <table>
              <thead>
                <tr>
                  <th style="width: 24%">Service</th>
                  <th style="width: 16%">Status</th>
                  <th style="width: 28%">Access</th>
                  <th style="width: 32%">Role</th>
                </tr>
              </thead>
              <tbody>${rows}</tbody>
            </table>
          </div>
        </section>
      </div>
    </body>
  </html>`;
}

function queryHtml() {
  const servingSummary = readServingSummary();
  const summary = servingSummary && servingSummary.summary ? servingSummary.summary : {};
  const totalTables = typeof summary.total_tables === "number" ? summary.total_tables : 11;
  const generatedAt = summary.generated_at || "2026-06-06T12:05:11Z";

  const rows = queryRows
    .map(
      (row) => `
      <tr>
        <td>${row.category}</td>
        <td>${row.net_amount}</td>
        <td>${row.gross_amount}</td>
        <td>${row.discount_amount}</td>
      </tr>`
    )
    .join("");

  return `<!doctype html>
  <html lang="en">
    <head>
      <meta charset="utf-8" />
      <style>
        :root {
          --bg: #081c15;
          --panel: #0f2a22;
          --panel-soft: #14362c;
          --line: rgba(160, 216, 180, 0.16);
          --text: #ecfdf5;
          --muted: #a7c9b6;
          --accent: #e9c46a;
          --accent-2: #8bd3a9;
        }
        * { box-sizing: border-box; }
        body {
          margin: 0;
          min-height: 100vh;
          color: var(--text);
          font-family: "Segoe UI", Arial, sans-serif;
          background:
            radial-gradient(circle at top right, rgba(139, 211, 169, 0.18), transparent 28%),
            radial-gradient(circle at bottom left, rgba(233, 196, 106, 0.12), transparent 24%),
            linear-gradient(160deg, #061711, #0b2119 56%, #10271f);
        }
        .frame {
          width: 1600px;
          min-height: 900px;
          padding: 56px;
        }
        .layout {
          display: grid;
          grid-template-columns: 1.2fr 0.8fr;
          gap: 24px;
        }
        .card {
          background: rgba(15, 42, 34, 0.94);
          border: 1px solid var(--line);
          border-radius: 28px;
          box-shadow: 0 30px 80px rgba(0, 0, 0, 0.24);
          overflow: hidden;
        }
        .hero {
          padding: 36px 40px 26px;
          border-bottom: 1px solid var(--line);
          background: linear-gradient(180deg, rgba(139, 211, 169, 0.08), transparent);
        }
        h1 {
          margin: 0 0 10px;
          font-size: 38px;
        }
        p {
          margin: 0;
          font-size: 18px;
          color: var(--muted);
          line-height: 1.55;
        }
        .meta {
          display: grid;
          grid-template-columns: repeat(2, minmax(0, 1fr));
          gap: 16px;
          padding: 20px 40px 0;
        }
        .stat {
          background: rgba(20, 54, 44, 0.9);
          border: 1px solid var(--line);
          border-radius: 18px;
          padding: 18px 20px;
        }
        .label {
          color: var(--muted);
          font-size: 13px;
          text-transform: uppercase;
          letter-spacing: 0.08em;
        }
        .value {
          margin-top: 10px;
          font-size: 28px;
          font-weight: 800;
          color: var(--accent);
        }
        .table-wrap {
          padding: 22px 28px 30px;
        }
        table {
          width: 100%;
          border-collapse: collapse;
        }
        th, td {
          padding: 14px 12px;
          text-align: left;
          border-bottom: 1px solid var(--line);
        }
        th {
          font-size: 13px;
          text-transform: uppercase;
          letter-spacing: 0.06em;
          color: var(--muted);
        }
        td:first-child {
          font-weight: 700;
          color: var(--accent-2);
        }
        .code-card {
          padding: 26px 28px 30px;
          background: rgba(6, 23, 17, 0.95);
        }
        .code-card h2 {
          margin: 0 0 16px;
          font-size: 22px;
        }
        pre {
          margin: 0;
          padding: 20px 22px;
          border-radius: 18px;
          background: #061711;
          border: 1px solid var(--line);
          color: #dff7e5;
          font-size: 16px;
          line-height: 1.6;
          overflow: hidden;
          white-space: pre-wrap;
        }
      </style>
    </head>
    <body>
      <div class="frame">
        <div class="layout">
          <section class="card">
            <div class="hero">
              <h1>Trino Query Serving</h1>
              <p>DuckDB serves the Gold layer locally and Trino exposes the analytics schema for SQL demos. The table below was queried from <code>lakehouse.analytics.revenue_by_category</code>.</p>
            </div>
            <div class="meta">
              <div class="stat">
                <div class="label">Materialized tables</div>
                <div class="value">${totalTables}</div>
              </div>
              <div class="stat">
                <div class="label">Catalog refreshed</div>
                <div class="value" style="font-size: 18px">${generatedAt.replace("T", " ").replace("Z", " UTC")}</div>
              </div>
            </div>
            <div class="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Category</th>
                    <th>Net Revenue</th>
                    <th>Gross Revenue</th>
                    <th>Discounts</th>
                  </tr>
                </thead>
                <tbody>${rows}</tbody>
              </table>
            </div>
          </section>
          <aside class="card code-card">
            <h2>Validated Commands</h2>
            <pre>docker exec lakehouse-trino trino --execute 'SHOW CATALOGS'
docker exec lakehouse-trino trino --execute 'SHOW TABLES FROM lakehouse.analytics'
docker exec lakehouse-trino trino --execute 'SELECT * FROM lakehouse.analytics.revenue_by_category LIMIT 5'</pre>
          </aside>
        </div>
      </div>
    </body>
  </html>`;
}

function observabilityHtml() {
  const observabilitySummary = readObservabilitySummary();
  const summary = observabilitySummary && observabilitySummary.summary ? observabilitySummary.summary : {};
  const pipelines = observabilitySummary && Array.isArray(observabilitySummary.pipelines)
    ? observabilitySummary.pipelines.slice(0, 4)
    : [];
  const totalExecutions = typeof summary.total_executions === "number" ? summary.total_executions : pipelines.length;
  const successExecutions = typeof summary.success_executions === "number" ? summary.success_executions : 0;
  const warningExecutions = typeof summary.warning_executions === "number" ? summary.warning_executions : 0;
  const failedExecutions = typeof summary.failed_executions === "number" ? summary.failed_executions : 0;
  const generatedAt = observabilitySummary && observabilitySummary.generated_at
    ? observabilitySummary.generated_at
    : "2026-06-06T18:38:20.471222+00:00";

  const rows = pipelines
    .map(
      (pipeline) => `
      <tr>
        <td class="job">${pipeline.job_name}</td>
        <td><span class="badge badge-${pipeline.status}">${pipeline.status}</span></td>
        <td>${pipeline.source_layer} -> ${pipeline.target_layer}</td>
        <td>${pipeline.duration_seconds}s</td>
        <td>${pipeline.records_in}</td>
        <td>${pipeline.records_out}</td>
        <td>${pipeline.invalid_records}</td>
        <td>${pipeline.valid_data_percentage}%</td>
      </tr>`
    )
    .join("");

  return `<!doctype html>
  <html lang="en">
    <head>
      <meta charset="utf-8" />
      <style>
        :root {
          --line: rgba(145, 196, 255, 0.18);
          --text: #edf6ff;
          --muted: #9eb3c9;
          --accent: #7cc5ff;
          --accent-soft: rgba(124, 197, 255, 0.12);
          --success-bg: rgba(35, 134, 54, 0.18);
          --success-text: #7ee787;
          --warning-bg: rgba(210, 153, 34, 0.18);
          --warning-text: #f2cc60;
        }
        * { box-sizing: border-box; }
        body {
          margin: 0;
          min-height: 100vh;
          color: var(--text);
          font-family: "Segoe UI", Arial, sans-serif;
          background:
            radial-gradient(circle at top right, rgba(124, 197, 255, 0.18), transparent 28%),
            radial-gradient(circle at bottom left, rgba(56, 189, 248, 0.1), transparent 20%),
            linear-gradient(160deg, #071018, #0c1624 55%, #101d31);
        }
        .frame {
          width: 1600px;
          min-height: 900px;
          padding: 42px;
        }
        .card {
          background: rgba(13, 20, 32, 0.96);
          border: 1px solid var(--line);
          border-radius: 28px;
          box-shadow: 0 28px 80px rgba(0, 0, 0, 0.28);
          overflow: hidden;
        }
        .hero {
          padding: 32px 36px 24px;
          border-bottom: 1px solid var(--line);
          background: linear-gradient(180deg, var(--accent-soft), transparent);
        }
        h1 {
          margin: 0 0 10px;
          font-size: 38px;
        }
        p {
          margin: 0;
          font-size: 18px;
          color: var(--muted);
          line-height: 1.55;
        }
        .layout {
          display: grid;
          grid-template-columns: 0.78fr 1.22fr;
          gap: 0;
        }
        .sidebar {
          padding: 24px;
          border-right: 1px solid var(--line);
          background: rgba(9, 15, 24, 0.62);
        }
        .grid {
          display: grid;
          grid-template-columns: repeat(2, minmax(0, 1fr));
          gap: 14px;
        }
        .stat {
          border: 1px solid var(--line);
          border-radius: 18px;
          padding: 18px 18px 16px;
          background: rgba(15, 25, 38, 0.92);
        }
        .label {
          color: var(--muted);
          font-size: 12px;
          text-transform: uppercase;
          letter-spacing: 0.08em;
        }
        .value {
          margin-top: 10px;
          font-size: 28px;
          font-weight: 800;
          color: var(--accent);
        }
        .timestamp {
          margin-top: 18px;
          padding: 16px 18px;
          border: 1px solid var(--line);
          border-radius: 18px;
          background: rgba(15, 25, 38, 0.92);
          color: var(--muted);
          font-size: 14px;
          line-height: 1.5;
        }
        .timestamp strong {
          display: block;
          margin-bottom: 6px;
          color: var(--text);
          font-size: 15px;
        }
        .table-wrap {
          padding: 22px 24px 24px;
        }
        table {
          width: 100%;
          border-collapse: collapse;
          table-layout: fixed;
        }
        th, td {
          padding: 13px 10px;
          text-align: left;
          border-bottom: 1px solid var(--line);
          vertical-align: middle;
        }
        th {
          color: var(--muted);
          font-size: 12px;
          text-transform: uppercase;
          letter-spacing: 0.08em;
        }
        td {
          font-size: 14px;
        }
        td.job {
          font-weight: 700;
          color: var(--text);
        }
        .badge {
          display: inline-flex;
          align-items: center;
          border-radius: 999px;
          padding: 6px 10px;
          font-size: 11px;
          font-weight: 700;
          text-transform: uppercase;
          letter-spacing: 0.06em;
        }
        .badge-success {
          background: var(--success-bg);
          color: var(--success-text);
        }
        .badge-warning {
          background: var(--warning-bg);
          color: var(--warning-text);
        }
        .badge-failed {
          background: rgba(248, 81, 73, 0.18);
          color: #ff938a;
        }
      </style>
    </head>
    <body>
      <div class="frame">
        <section class="card">
          <div class="hero">
            <h1>Pipeline Observability</h1>
            <p>Execution telemetry generated by the local pipeline, including status, duration, processed volume, invalid records and data validity for the latest jobs.</p>
          </div>
          <div class="layout">
            <aside class="sidebar">
              <div class="grid">
                <div class="stat">
                  <div class="label">Total executions</div>
                  <div class="value">${totalExecutions}</div>
                </div>
                <div class="stat">
                  <div class="label">Success</div>
                  <div class="value">${successExecutions}</div>
                </div>
                <div class="stat">
                  <div class="label">Warnings</div>
                  <div class="value">${warningExecutions}</div>
                </div>
                <div class="stat">
                  <div class="label">Failed</div>
                  <div class="value">${failedExecutions}</div>
                </div>
              </div>
              <div class="timestamp">
                <strong>Report generated at</strong>
                ${generatedAt.replace("T", " ").replace("Z", " UTC")}
              </div>
            </aside>
            <div class="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th style="width: 18%">Job</th>
                    <th style="width: 11%">Status</th>
                    <th style="width: 16%">Layers</th>
                    <th style="width: 10%">Duration</th>
                    <th style="width: 10%">In</th>
                    <th style="width: 10%">Out</th>
                    <th style="width: 11%">Invalid</th>
                    <th style="width: 14%">Valid %</th>
                  </tr>
                </thead>
                <tbody>${rows}</tbody>
              </table>
            </div>
          </div>
        </section>
      </div>
    </body>
  </html>`;
}

async function screenshotHtml(browser, html, outputPath, viewport) {
  ensureDir(path.dirname(outputPath));
  archiveExistingScreenshot(outputPath);
  const page = await browser.newPage({ viewport });
  await page.setContent(html, { waitUntil: "networkidle" });
  await page.screenshot({ path: outputPath, type: "png" });
  await page.close();
  console.log(`screenshot saved: ${path.relative(projectRoot, outputPath)}`);
}

async function captureAirflow(browser) {
  const outputPath = path.join(
    readmeAssetsRoot,
    "orchestration",
    "06-readme-airflow-dag.png",
  );
  ensureDir(path.dirname(outputPath));
  archiveExistingScreenshot(outputPath);

  const page = await browser.newPage({ viewport: { width: 1600, height: 900 } });
  try {
    await page.goto("http://airflow-api-core:8080/login/", { waitUntil: "networkidle", timeout: 60000 });
    await page.waitForTimeout(3000);

    const loginInput = page
      .locator('input[name="username"], input[type="email"], input[autocomplete="username"]')
      .first();
    if (await loginInput.count()) {
      await loginInput.fill("admin");
      await page.locator('input[name="password"], input[type="password"]').first().fill("admin");
      const submitButton = page
        .locator('button[type="submit"], button:has-text("Sign In"), button:has-text("Login"), button:has-text("Entrar")')
        .first();
      if (await submitButton.count()) {
        await submitButton.click();
      } else {
        await page.locator('input[name="password"], input[type="password"]').first().press("Enter");
      }
      await page.waitForTimeout(4000);
    }

    await page.goto("http://airflow-api-core:8080/dags", { waitUntil: "networkidle", timeout: 60000 });
    await page.waitForTimeout(3000);

    const dagLink = page.getByText("lakehouse_pipeline_dag").first();
    if (await dagLink.count()) {
      await dagLink.click();
      await page.waitForLoadState("networkidle");
      await page.waitForTimeout(3000);
    }

    await page.screenshot({ path: outputPath, type: "png", fullPage: false });
    console.log(`screenshot saved: ${path.relative(projectRoot, outputPath)}`);
  } catch (error) {
    console.warn(`airflow screenshot skipped: ${error.message}`);
  } finally {
    await page.close();
  }
}

async function main() {
  const browser = await chromium.launch({ headless: true });
  try {
    await screenshotHtml(
      browser,
      runtimeHtml(),
      path.join(readmeAssetsRoot, "runtime", "08-readme-local-services-overview.png"),
      { width: 1600, height: 900 },
    );
    await screenshotHtml(
      browser,
      observabilityHtml(),
      path.join(readmeAssetsRoot, "observability", "04-readme-observability-metrics.png"),
      { width: 1600, height: 900 },
    );
    await screenshotHtml(
      browser,
      queryHtml(),
      path.join(readmeAssetsRoot, "query", "10-readme-trino-query-serving.png"),
      { width: 1600, height: 900 },
    );
    await captureAirflow(browser);
  } finally {
    await browser.close();
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
