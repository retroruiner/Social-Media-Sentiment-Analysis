# How to Run the Project Locally

## Prerequisites

Make sure you have Python installed (Python 3.9 or later recommended).

### Verify Python Installation:
```bash
python --version
```

## 1. Clone the Repository

Clone the project to your local system:
```bash
git clone https://github.com/retroruiner/Social-Media-Sentiment-Analysis.git
cd <project-folder>
```

## 2. Set Up a Virtual Environment

It is recommended to use a virtual environment:

```bash
python -m venv venv
source venv/bin/activate  # On Windows use: venv\Scripts\activate
```

## 3. Install Dependencies

Install required libraries from `requirements.txt`:

```bash
pip install -r requirements.txt
```

## 4. Run the Flask API

Run the Flask backend application first:

```bash
python app.py
```

The Flask app will be accessible at [`http://127.0.0.1:5000`](http://127.0.0.1:5000).

## 5. Run the Dash Application

Open another terminal window, activate the same virtual environment, and run the Dash frontend application:

```bash
python dashboard.py
```

The Dash application will be accessible at [`http://127.0.0.1:8050`](http://127.0.0.1:8050).

## 6. Access the Application

Navigate to [`http://127.0.0.1:8050`](http://127.0.0.1:8050) in your browser to view and interact with the Sentiment Analysis dashboard.

---

## Notes
- Ensure both Flask and Dash apps run simultaneously.
- Adjust configurations in `app.py` or the Dash app file as needed.
- If encountering issues, verify all packages in `requirements.txt` are correctly installed.

