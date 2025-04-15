from flask import Flask
from dashboard import app as dash_app
from flask_cors import CORS
from api_routes import api
from database.models import Base, engine

# Ensure tables are created at startup
Base.metadata.create_all(engine)

server = Flask(__name__)
CORS(server)  # Optional if CORS issues arise

# Register the API routes; you can choose a prefix (here "/api") or not.
server.register_blueprint(api, url_prefix="/api")

# Attach Dash to the same Flask server
dash_app.server = server

if __name__ == "__main__":
    # This block is only used when running locally
    server.run(debug=True, host="0.0.0.0", port=5000)
