from flask import Blueprint, render_template
import json # Import the json library

# Create a Blueprint named 'views'
views = Blueprint('views', __name__)

# --- Load the content from the JSON file ---
# This opens the file, reads its contents, and converts it into a Python dictionary.
with open('website/content.json', 'r') as f:
    content_data = json.load(f)

# Route for the home page
@views.route('/')
def home():
    """Renders the home page."""
    # Pass the 'home' section of our data to the template
    return render_template("home.html", content=content_data['home'])

# Route for the projects page
@views.route('/projects')
def projects():
    """Renders the projects page."""
    # Pass the 'projects' list to the template
    return render_template("projects.html", projects=content_data['projects'])

# Route for the contact page
@views.route('/contact')
def contact():
    """Renders the contact page."""
    # Pass the 'contact' section to the template
    return render_template("contact.html", content=content_data['contact'])