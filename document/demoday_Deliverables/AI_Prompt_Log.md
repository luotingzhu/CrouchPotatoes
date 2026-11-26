# FinBrief · AI Prompt Log (Software Development)

- Project: FinBrief (formerly Domel) — Motor Market Intelligence AI Assistant
- Team: CrouchPotatoes ｜ Repository: https://github.com/luotingzhu/CrouchPotatoes
- Scope: 14 Oct 2026 – 26 Nov 2026, 43 development-related instructions
- Tooling: Trae (AI coding assistant) — conversational coding plus file editing and command execution

> **Note on sources**
> - Entries 24–43 come from the current development session and are kept verbatim (translated here into English).
> - Entries 1–23 are from earlier sessions whose raw input was not stored locally: Trae's session store is encrypted and cannot be exported. They were reconstructed from the session record and the artefacts they produced; wording is close to the original but may differ in places.
> - Only development-related instructions are included. Purely conversational messages ("it's on", "I clicked start") appear only when they affected the development flow.
> - Every entry lists its **output**, so it can be checked against the files and commits in the repository.
> - All prompts were originally written in Chinese and are translated below.

---

## 1. Architecture and Data Layer (Phase 1)

| # | Prompt (original instruction, translated) | Output |
|---|---|---|
| 1 | Project brief: build a B2B "Domel Motor Market Intelligence AI Assistant"; produce a complete PostgreSQL DDL (partitioning, indexes, foreign keys), the ORM models, and the service layer for multi-source ingestion plus AI report vectorisation; output step by step with Chinese comments | `db/schema.sql` (350 lines: monthly partitions, GIN/HNSW indexes), `app/db/models.py`, `services/ingestion_service.py`, `services/report_service.py` |
| 2 | How do I actually call the AI APIs? | `services/ai_client.py` (separate credentials/endpoints for chat and embeddings), `.env.example` |

## 2. Authentication and Base Frontend Interaction (Phase 2)

| # | Prompt (original instruction, translated) | Output |
|---|---|---|
| 3 | Request failed with 500 (plus the registration error mentioned earlier) | Traced the backend exception and completed the register/login path |
| 4 | When a new user registers, the error cannot be displayed properly (it shows `[object Object]`). Check the whole registration section and fix it | Frontend `formatApiError()` that turns pydantic 422 `detail` arrays into readable messages |
| 5 | Restrict the email format | Shared email validation rules on both sides (length, local part, domain labels, TLD, consecutive dots) |
| 6 | Add a Chinese/English toggle to the whole site, top-right corner, CN/EN | `I18N.zh/en` dictionaries, `setLang()`, language switch on the top bar and login page |
| 7 | In Preferences, add an "admin mode": only after enabling it and entering the password `Ch11seburger` should the "backend sync" module appear | Redis admin flag, `POST /api/system/admin/unlock`, frontend switch and password box |

## 3. Ingestion, Reports and Data Authenticity (Phase 3)

| # | Prompt (original instruction, translated) | Output |
|---|---|---|
| 8 | Find out why no report can be generated after fetching | Root cause: subscribed modules had no data within the 72-hour window |
| 9 | It should warn when only stale data is available — and I fetched on the Steven account, so check what happened | Report gained `data_stale / latest_source_time / stale_module_codes`; frontend stale-data banner |
| 10 | Not solved: after fetching the latest news the report still says the data is from 10-14 | Corrected the message to list stale modules and suggest modules that do have fresh data |
| 11 | Fetch from the default sources according to the user's selected modules, so every module has today's data after clicking fetch | Ingestion dispatches each enabled source to its assigned module and guarantees same-day records |
| 12 | Make sure the news sources are real, then restructure reports: table of contents first, then chaptered content | Removed sample sources; `report_service` emits contents + chapters; frontend `.toc` / `.chapter` rendering |
| 13 | Translate the raw-data news titles when the interface is switched to English | `POST /api/sources/raw/translate`, 7-day Redis cache, asynchronous title replacement |
| 14 | Data sources are not translated either — check the language handling across the whole app | `SOURCE_NAME_EN` mapping + `sourceLabel()`, localised type badges |

## 4. Branding, Delivery and Deployment (Phase 4)

| # | Prompt (original instruction, translated) | Output |
|---|---|---|
| 15 | I want to build a simple introduction site and expose it publicly via intranet tunnelling; explain how tunnelling works | Landing page plan plus a tunnelling explainer (cpolar / cloudflared / frp) |
| 16 | Rename the software to finbrief, with `logo.png` / `logo_trans.png`, and update the product | Rebranded assets, page titles and copy |
| 17 | Write a one-click script that brings up the database and prints the access URL before pausing | `start.bat` + `start.ps1` (containers → health checks → backend → print URLs → pause) |
| 18 | What is `start.ps1`? | Walkthrough of each section and its execution order |
| 19 | `https://yyann2006.github.io/finbrief-site/` is the landing page built by a teammate — can you import it? | Landing page imported into `static/site/`, asset paths localised |
| 20 | Fix it: the demo link opens something unrelated instead of my app | Demo button points to the project root `/`, external redirect removed |
| 21 | Do not prefill the username and password | Prefilled values removed from the login form |
| 22 | You can delete the sample sources and add a few more | Sample sources removed; Xinhua and National Bureau of Statistics crawler profiles added |
| 23 | Add English copy to the backend logs | `EventBus` gained `message_en`; 27 events updated; frontend picks text by language |

## 5. Public Exposure / Tunnelling Troubleshooting (16 Nov)

| # | Prompt (original instruction, translated) | Output |
|---|---|---|
| 24 | Let's set up intranet tunnelling — something stable in mainland China. Cloudflare did not connect for me | Reachability tests (cpolar reachable, natapp not), downloaded and deployed the cpolar client |
| 25 | I don't have a phone number registered in my own name — is there an option without one? | Evaluated providers that do not require a phone number |
| 26 | For the record, I have a SakuraFrp account and the client installed | Switched to the installed SakuraFrp launcher, started its service and inspected the tunnel configuration |
| 27 | It's on (tunnel started) | Read the tunnel list through the API (node #48, remote port 48540); confirmed the node blocks plaintext HTTP (501) |
| 28 | Does it need ICP filing? | Clarified that filing only applies to custom domains on domestic nodes, and gave two workable paths |
| 29 | How do I access it? | Explained that HTTP tunnels require an owned domain (`.natfrp.cloud` rejected with 403); switched to TCP + overseas node |

## 6. Mobile Layout and Production Fixes (16–17 Nov)

| # | Prompt (original instruction, translated) | Output |
|---|---|---|
| 30 | My friend is opening it on a phone and the resolution/layout does not fit | Added `≤860px` / `≤420px` breakpoints: horizontal nav, single-column grids, in-card table scrolling, block-level login card; fixed the stretched grid row that made the nav bar too tall |
| 31 | The raw-data link overflows its box on mobile | `overflow-wrap:anywhere` on `.list .item .m`; width limit on traced source links |
| 32 | Knowledge-base chat seems broken | Reproduced end to end (API fine); found the dialog pushed off-screen by a long session list on narrow screens; capped the list height and auto-scrolled to the dialog |
| 33 | (Uploaded `INFO_....txt` backend log) | Diagnosed `redis.exceptions.ConnectionError ... localhost:6379`; confirmed the WSL VM had idled out and stopped PostgreSQL/Redis |
| 34 | A teammate changed some code and put it in `v2\CrouchPotatoes-2`; after running it for a while it broke like this | Compared the two copies, confirmed container-name/port collisions and volume ownership, restored v1 and verified the data was intact |
| 35 | Look at what changed — then I'll decide | Per-file diff: 9 capabilities only in v2, 6 fixes only in v1, 2 conflicting files |

## 7. Version Merge and Global Quality (17–19 Nov)

| # | Prompt (original instruction, translated) | Output |
|---|---|---|
| 36 | Merge the v2 changes into v1 in the suggested order | Merged: plain-text answers, structured citations, session/report deletion, ingestion overview, purge, per-module quotas + language fallback; kept v1's mobile layout, English logs and 503 handling |
| 37 | "Ingestion overview" is not translated — check the translations globally | Added the missing `btn.delete`; localised module/source/type labels in the stats card; localised default session titles; extended the error-message map; audited 132 keys with zero gaps |
| 38 | Compile a work-time log from all conversations and file history | `document/工作时间日志.md` (built from Git commits, file timestamps and session records) |
| 39 | The project is ending — write a reflection, in English, PDF format | `document/FinBrief_Reflection.pdf` (2 pages) |
| 40 | Continue the work log — the platform report contains very little about my part; follow that report's style, in English, with GitHub as evidence | `document/Individual_Contribution_Report_LuotingZhu.pdf` (4 pages: day-by-day log, architecture decisions, leadership, promo video and demo meeting) |
| 41 | `ConnectionRefusedError: [WinError 1225] The remote computer refused the network connection.` | Reproduced and confirmed WSL shutdown → containers stopped; added a keep-alive process and dual health checks; verified still reachable after 40 s idle |
| 42 | I clicked start | Verified `start.ps1` behaviour: detected port 8000 already in use, skipped the redundant launch and reused the running backend; confirmed 234 records / 10 sources / 998 vectors / 4 reports |
| 43 | Export all my software-development prompts into a document | This document |

---

## 8. Statistics by Theme

| Theme | Count | Notes |
|---|---|---|
| Architecture and data layer | 2 | DDL/ORM/service layer, AI client wrapper |
| Authentication and frontend interaction | 5 | 500/422 errors, email validation, i18n, admin mode |
| Ingestion, reports, data authenticity | 7 | Report generation failure, stale data, real sources, TOC/chapters, translation |
| Branding and delivery | 9 | Rename, logo, one-click start, landing page import, English logs |
| Tunnelling | 6 | Provider selection, node restrictions, access method |
| Mobile and production fixes | 6 | Responsive layout, long links, chat diagnosis, WSL outage, v2 collision |
| Merge, quality and documentation | 8 | Feature merge, global translation, work log, reflection, contribution report |

## 9. Verifiable Mapping

| Output | Location |
|---|---|
| Database schema | `db/schema.sql` |
| Backend services | `backend/app/services/*.py`, `backend/app/api/*.py` |
| Frontend SPA | `backend/app/static/index.html` |
| Start scripts | `start.bat`, `start.ps1` |
| Deliverable documents | `document/` (technical proposal, prototype report, MVP record, 5 weekly logs, review log, work-time log, reflection, contribution report) |
| Commit timeline | `https://github.com/luotingzhu/CrouchPotatoes` (7 public commits; 1 additional commit still local) |
