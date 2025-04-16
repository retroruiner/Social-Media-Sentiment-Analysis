import os
from flask import Flask
from flask_cors import CORS
from api_routes import api
from database.models import Base, engine
from dashboard import init_dashboard  # We'll create this function in dashboard.py

# Ensure tables are created at startup
Base.metadata.create_all(engine)

# Create and configure the Flask server
server = Flask(__name__)
CORS(server)  # Optional if CORS issues arise

# Register API routes under /api
server.register_blueprint(api, url_prefix="/api")

# Initialize your Dash app by passing the Flask server
dash_app = init_dashboard(server)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    server.run(debug=True, host="0.0.0.0", port=port)
