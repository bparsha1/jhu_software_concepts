## Personal Portfolio Website (Module 1)

This folder contains the source code for a personal portfolio website built with Python and the Flask web framework.

### How to Run This Site

To run this website locally, please follow these steps:

1.  **Prerequisites:**
    * Ensure you have Python 3.10 or newer installed on your system.
    * It is highly recommended to use a virtual environment.

2.  **Clone the Repository:**
    Clone the `jhu_software_concepts` repository to your local machine if you haven't already.

3.  **Navigate to the Project Directory:**
    Open your terminal and change your directory to this `module_1` folder.
    ```bash
    cd path/to/jhu_software_concepts/module_1
    ```

4.  **Set Up Virtual Environment and Install Dependencies:**
    Create and activate a Python virtual environment, then install the required packages from the `requirements.txt` file.
    ```bash
    # Create a virtual environment
    python -m venv venv

    # Activate the environment
    # On macOS/Linux:
    source venv/bin/activate
    # On Windows:
    .\venv\Scripts\activate

    # Install the necessary packages
    pip install -r requirements.txt
    ```

5.  **Run the Application:**
    Execute the `run.py` script to start the Flask development server.
    ```bash
    python run.py
    ```

6.  **Access the Website:**
    The server will start and listen on port 8080. Open your web browser and navigate to:
    http://127.0.0.1:8080/

    The site should now be running locally.