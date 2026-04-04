# Adur & Worthing Live Bus Tracker

A free, open-source live bus departure board and vehicle tracking website for the **Adur & Worthing** area of West Sussex, UK.

- 🗺 **Interactive Leaflet map** showing live bus positions and stop markers  
- 🕐 **Real-time departure boards** for any stop, one click away  
- 📱 **Mobile-friendly** responsive layout  
- 🔑 **API key kept safe** — never exposed in frontend code  
- 💸 **Entirely free to run** using GitHub Pages + Vercel/Render free tiers  

Data from the [Bus Open Data Service (BODS)](https://data.bus-data.dft.gov.uk/) — UK Department for Transport.

---

## Project Structure

```
adur-worthing-bus/
├── index.html          ← Frontend: main page
├── style.css           ← Frontend: all styles
├── app.js              ← Frontend: map, live buses, departure board
│
├── api/
│   └── main.py         ← Backend: FastAPI proxy for BODS API
│
├── requirements.txt    ← Python dependencies
├── vercel.json         ← Vercel deployment config (use this OR render.yaml)
├── render.yaml         ← Render deployment config (alternative to Vercel)
└── README.md           ← This file
```

**Frontend** is hosted on **GitHub Pages** (free static hosting).  
**Backend** is hosted on **Vercel** or **Render** (free Python hosting).

---

## Step 1 — Get a BODS API Key (5 minutes)

The UK Department for Transport provides free API access to live bus data.

1. Go to **[https://data.bus-data.dft.gov.uk/](https://data.bus-data.dft.gov.uk/)**
2. Click **"Create an account"** and register with your email address
3. Once logged in, click your name (top-right) → **"Account settings"** → **"API key"**
4. Copy the API key — it looks like a long string of letters and numbers
5. **Keep this safe** — you'll add it as a secret environment variable in Step 3 or 4, never in code

---

## Step 2 — Set Up the Code Repository

### Option A — Fork on GitHub (easiest)

1. Go to this repository on GitHub and click **"Fork"** (top-right)
2. GitHub will create a copy of the repo under your account
3. You now have `https://github.com/YOUR-USERNAME/adur-worthing-bus`

### Option B — Upload files manually

1. Create a new repository on GitHub: click **"+"** → **"New repository"**
2. Name it `adur-worthing-bus`, set it to **Public**, click **"Create repository"**
3. Upload all the files from this project using **"Add file"** → **"Upload files"**

---

## Step 3 — Deploy the Backend to Vercel (recommended)

Vercel is the easiest free option for the Python API backend.

### 3.1 — Install Vercel CLI

You need [Node.js](https://nodejs.org/) installed first.

```bash
npm install -g vercel
```

### 3.2 — Deploy

From the root folder of the project:

```bash
vercel
```

Follow the prompts:
- **Set up and deploy?** → Yes
- **Which scope?** → Your personal account
- **Link to existing project?** → No
- **Project name** → `adur-worthing-bus-api` (or anything you like)
- **Directory** → `.` (current directory)

Vercel will deploy and give you a URL like `https://adur-worthing-bus-api.vercel.app`.

### 3.3 — Add your BODS API Key as a Secret

**Never** paste the API key into code. Instead, add it as a Vercel secret:

```bash
vercel env add BODS_API_KEY
```

When prompted, paste your BODS API key and press Enter. Select **"Production"** environment.

Then add your GitHub Pages URL (you'll get this in Step 5):

```bash
vercel env add ALLOWED_ORIGIN
```

Paste `https://YOUR-GITHUB-USERNAME.github.io` and press Enter.

### 3.4 — Redeploy with environment variables applied

```bash
vercel --prod
```

Your backend is now live. Test it by visiting:
```
https://adur-worthing-bus-api.vercel.app/
```
You should see: `{"status": "ok", "bods_key_configured": true, ...}`

**Alternative — Vercel Dashboard (no CLI needed):**
1. Go to [https://vercel.com](https://vercel.com) and sign in with GitHub
2. Click **"Add New Project"** → import your GitHub repository
3. In the deploy settings, expand **"Environment Variables"** and add:
   - `BODS_API_KEY` = your BODS API key
   - `ALLOWED_ORIGIN` = `https://YOUR-USERNAME.github.io`
4. Click **"Deploy"**

---

## Step 3 (Alternative) — Deploy the Backend to Render

If you prefer Render over Vercel:

1. Create a free account at [https://render.com](https://render.com)
2. Click **"New +"** → **"Web Service"**
3. Connect your GitHub account and select your `adur-worthing-bus` repository
4. Render should auto-detect the `render.yaml` file
5. Set the following environment variables in Render's dashboard:
   - **Key:** `BODS_API_KEY` → **Value:** your BODS API key
   - **Key:** `ALLOWED_ORIGIN` → **Value:** `https://YOUR-USERNAME.github.io`
6. Click **"Create Web Service"**

Your Render URL will look like: `https://adur-worthing-bus-api.onrender.com`

> **Note:** Render free tier sleeps after 15 minutes of inactivity. The first request after sleeping takes ~30 seconds. This is fine for occasional use.

---

## Step 4 — Configure the Frontend with Your Backend URL

Open `app.js` and find this section near the top:

```javascript
const CONFIG = {
  // Change this to your Vercel or Render URL:
  API_BASE_URL: "https://YOUR-BACKEND-URL-HERE.vercel.app",
  ...
```

Replace `https://YOUR-BACKEND-URL-HERE.vercel.app` with your actual backend URL from Step 3, e.g.:

```javascript
API_BASE_URL: "https://adur-worthing-bus-api.vercel.app",
```

Save the file and commit/push to GitHub.

---

## Step 5 — Deploy the Frontend to GitHub Pages

1. Go to your GitHub repository
2. Click **"Settings"** → **"Pages"** (in the left sidebar)
3. Under **"Source"**, select **"Deploy from a branch"**
4. Set **Branch** to `main` and **Folder** to `/ (root)`
5. Click **"Save"**

GitHub will build your site. After a minute or two, it will be available at:
```
https://YOUR-GITHUB-USERNAME.github.io/adur-worthing-bus/
```

GitHub shows you the exact URL in the Pages settings page.

> If your repo is named differently (e.g. `bus-tracker`), the URL will be  
> `https://YOUR-USERNAME.github.io/bus-tracker/`

---

## Step 6 — Verify Everything Works

1. Visit your GitHub Pages URL
2. The map should load centred on Worthing
3. Blue dots (bus stops) should appear on the map — this means stop data loaded
4. After ~20 seconds, amber/orange squares (live buses) should appear
5. Click a blue stop dot → the departure board panel should slide open on the right

### Troubleshooting

| Problem | Likely cause | Fix |
|---|---|---|
| Map loads but no stops | Backend not reachable | Check API_BASE_URL in app.js; visit backend URL directly |
| `{"bods_key_configured": false}` on backend | BODS_API_KEY not set | Re-add env var and redeploy |
| "CORS error" in browser console | ALLOWED_ORIGIN wrong | Set it to your exact GitHub Pages URL in backend env vars |
| No live buses visible | BODS feed may have no data for the area at that time | Try at a peak hour; check BODS service status |
| Departures show "No departures found" | Stop has no imminent buses | Try a busier stop, or check if the stop ATCO code is correct |

---

## Local Development

To run everything locally for testing:

### Backend

```bash
# Create a virtual environment (optional but recommended)
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Create a .env file (never commit this!)
echo "BODS_API_KEY=your_key_here" > .env
echo "ALLOWED_ORIGIN=*" >> .env

# Start the backend
uvicorn api.main:app --reload --port 8000
```

The backend is now at `http://localhost:8000`.  
Test it: [http://localhost:8000/api/stops](http://localhost:8000/api/stops)

### Frontend

Temporarily change `app.js`:
```javascript
API_BASE_URL: "http://localhost:8000",
```

Then serve the frontend files with any static server:
```bash
# Python's built-in server
python -m http.server 3000
```

Open [http://localhost:3000](http://localhost:3000) in your browser.

**Remember to revert `API_BASE_URL` to your Vercel/Render URL before committing.**

---

## API Endpoints

Once deployed, these endpoints are available on your backend:

| Endpoint | Description | Cache |
|---|---|---|
| `GET /` | Health check | None |
| `GET /api/stops` | All bus stops in Adur & Worthing | 24 hours |
| `GET /api/vehicles` | Live bus positions (bounding box filtered) | 15 seconds |
| `GET /api/departures?stopId=ATCO` | Departures for a single stop | 30 seconds |

### Example responses

**`/api/stops`**
```json
{
  "stops": [
    {
      "atco_code": "1400A0001",
      "name": "Worthing Rail Station",
      "latitude": 50.8123,
      "longitude": -0.3715
    }
  ],
  "count": 312
}
```

**`/api/vehicles`**
```json
{
  "vehicles": [
    {
      "vehicle_ref": "SN19ABC",
      "service_ref": "7",
      "operator_ref": "SCSC",
      "destination": "Brighton",
      "latitude": 50.819,
      "longitude": -0.368,
      "bearing": 95.0,
      "delay_seconds": 120,
      "recorded_at": "2024-09-15T08:32:00+01:00"
    }
  ],
  "count": 24
}
```

**`/api/departures?stopId=1400A0001`**
```json
{
  "stop_name": "Worthing Rail Station",
  "departures": [
    {
      "service": "7",
      "destination": "Brighton",
      "aimed_departure": "2024-09-15T08:45:00+01:00",
      "expected_departure": "2024-09-15T08:47:00+01:00",
      "status": "Late",
      "delay_seconds": 120
    }
  ]
}
```

---

## Costs

| Service | Cost |
|---|---|
| GitHub Pages (frontend hosting) | **Free** |
| Vercel (backend, up to 100 GB bandwidth/month) | **Free** |
| Render (backend, 750 hours/month) | **Free** |
| OpenStreetMap tiles (Leaflet map) | **Free** |
| BODS API (DfT) | **Free** |
| NaPTAN stops API (DfT) | **Free** |
| **Total** | **£0/month** |

---

## Extending the Site

The code has clearly marked `FUTURE EXTENSION POINTS` comments throughout.

### Adding service alerts
1. Add a `/api/alerts` endpoint to `api/main.py` (see the comment there)
2. Fetch from the BODS SIRI-SX (Situation Exchange) feed
3. Display in a banner in `index.html`

### Adding route detail pages
1. Create `route.html` with a timetable layout
2. Add `/api/route?serviceRef=...` to the backend using BODS GTFS data
3. Link from the departure table rows in `app.js`

### Adding a "nearest stops" feature
1. Use `navigator.geolocation.getCurrentPosition()` in `app.js`
2. Filter `stops` by distance from user's location
3. Highlight the 3 nearest on the map and open the closest automatically

### Rebranding
All colours are CSS custom properties in `style.css` under `:root`. Change the values there to match any colour scheme. No other files need editing.

---

## Data Sources & Licences

- **Bus Open Data Service (BODS)** — [data.bus-data.dft.gov.uk](https://data.bus-data.dft.gov.uk/)  
  Operated by the Department for Transport. Free to use under the [Open Government Licence v3.0](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/).

- **NaPTAN** (National Public Transport Access Nodes) — DfT, OGL v3.0

- **OpenStreetMap** — © OpenStreetMap contributors, [ODbL](https://www.openstreetmap.org/copyright)

- **Leaflet.js** — [leafletjs.com](https://leafletjs.com/) — BSD 2-Clause Licence

---

## Contributing & Feedback

For bug reports, feature requests, or questions, open an issue on GitHub.

Pull requests are welcome — please keep the code plain HTML/CSS/JS on the frontend and pure FastAPI on the backend (no heavy frameworks) so it stays easy to maintain.
