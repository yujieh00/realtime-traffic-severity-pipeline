# Getting this project onto GitHub (first-timer's guide)

You've never used GitHub before — that's fine. This walks you through every
click, using **GitHub Desktop** (a visual app, no command line needed). Budget
about 30 minutes. When you're done, this project will be live at
`https://github.com/yujieh00/realtime-traffic-severity-pipeline` and pinned
to your profile for recruiters to see.

> Links throughout this guide already use your username, **yujieh00**.

---

## Part 1 — One-time setup (~10 min)

### 1.1 Sign in
- Go to <https://github.com> and sign in — your account is `yujieh00`.
- Optional: if you'd prefer a cleaner handle (recruiters see it), you can rename
  it in *Settings → Account*. Links break when you do, so decide before you push.
  `yujieh00` is perfectly fine to keep.

### 1.2 Install GitHub Desktop
- Download from <https://desktop.github.com> and install it.
- Open it, click **Sign in to GitHub.com**, and authorise it in the browser.
- When it asks about "Git config", just click **Continue** (the defaults are fine).

That's it for setup. You won't need the `git` command line at all.

---

## Part 2 — Publish this project (~10 min)

### 2.1 Add the project as a repository
1. In GitHub Desktop: **File → Add Local Repository**.
2. Click **Choose…** and select this folder:
   `realtime-traffic-severity-pipeline`.
3. It will say *"This directory does not appear to be a Git repository. Would you
   like to create a repository here instead?"* — click **create a repository**.
4. On the next screen:
   - **Name**: `realtime-traffic-severity-pipeline` (already filled in)
   - **Description**: `Real-time streaming ML pipeline: Kafka → Spark Structured Streaming → MLlib severity prediction → live dashboard`
   - Leave **Git ignore** as *None* (this repo already has a `.gitignore`).
   - **License**: *None* (this repo already has a `LICENSE` file).
   - Click **Create Repository**.

### 2.2 Make your first commit
A "commit" is a saved snapshot of your files.
1. On the left you'll see all the files listed as changes.
2. At the bottom-left, in the **Summary** box, type:
   `Initial commit: streaming traffic-severity pipeline`
3. Click the blue **Commit to main** button.

### 2.3 Push it to GitHub
1. Click **Publish repository** (top bar).
2. **Uncheck** "Keep this code private" if you want it public (a portfolio piece
   should be **public** so recruiters can see it).
3. Click **Publish Repository**.

Done — your code is now live. Click **View on GitHub** to see it in the browser.

> **Sanity check before you publish:** open the folder and confirm there's no
> `data/raw/` folder, no large CSVs beyond `data/sample/`, and no `models/`
> contents. The `.gitignore` already excludes these, but it's worth a glance.

---

## Part 3 — Make it look good (~10 min)

### 3.1 Add topics and a description on the repo page
On your repo's GitHub page, click the ⚙️ gear next to **About** (right side) and add:
- **Description**: same one-liner as above.
- **Topics**: `data-engineering`, `apache-kafka`, `apache-spark`,
  `spark-structured-streaming`, `pyspark`, `machine-learning`, `streaming`,
  `mllib`, `python`.

These make the repo searchable and signal your stack at a glance.

### 3.2 Pin the repository to your profile
1. Go to your profile: `https://github.com/yujieh00`.
2. Find **Customize your pins** (or **Pin repositories**).
3. Tick `realtime-traffic-severity-pipeline` and save. It now shows at the top of
   your profile.

### 3.3 Create your profile README
This is the "front page" that appears on your GitHub profile. A ready-made one is
in [`PROFILE_README.md`](PROFILE_README.md) in this folder.
1. On GitHub, click **+ → New repository** (top-right).
2. Name it **exactly** your username — e.g. `yujieh00`. GitHub will show a
   note: *"You found a secret! …this special repository…"* — that's what you want.
3. Tick **Add a README file**, then **Create repository**.
4. Open the new `README.md`, click the pencil ✏️ to edit, delete the placeholder
   text, and paste in the contents of `PROFILE_README.md` (edit the details
   first). Commit the change.

---

## Making changes later

Whenever you edit a file:
1. GitHub Desktop shows the change automatically.
2. Type a short summary (e.g. `Improve README results table`).
3. **Commit to main**, then click **Push origin**.

That three-step loop — *change → commit → push* — is 90% of everyday Git.

---

## If you'd rather use VS Code

VS Code has Git built in:
- Open the folder in VS Code.
- Click the **Source Control** icon on the left (branching icon).
- Click **Initialize Repository**, type a commit message, click **✓ Commit**.
- Click **Publish Branch** and choose **public**.
The concepts are identical to GitHub Desktop — commit, then push.

---

## Quick glossary

| Term | Plain meaning |
|------|---------------|
| **Repository (repo)** | A project folder that Git tracks. |
| **Commit** | A saved snapshot of your changes, with a message. |
| **Push** | Upload your commits to GitHub. |
| **Pull** | Download changes from GitHub. |
| **main** | The default branch (line of history) — you'll only need this one. |
| **`.gitignore`** | A list of files Git should never upload (data, models, secrets). |
