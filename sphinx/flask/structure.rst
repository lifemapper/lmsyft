Structure
######################################

Specify Network consists of four Docker containers running on a single EC2 instance.

The nginx and front-end containers support both the Analyst and Broker.  Two flask
containers, one for Analyst, and one for Broker, expose the APIs of each to different
subdomains of the same domain.  Code for each is in the flask_app.analyst and
flask_app.broker directories.  In each, the routes.py file defines the different
endpoints.




