from flask import Flask

def create_app():
    """
    Application factory function to create and configure the Flask app.
    """
    app = Flask(__name__)
    
    # Import the blueprint from the views file
    from .views import views
    
    # Register the blueprint with the app
    # All routes defined in the blueprint will be added to the app
    app.register_blueprint(views, url_prefix='/')
    
    return app