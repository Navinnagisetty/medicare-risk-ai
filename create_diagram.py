from diagrams import Diagram
from diagrams.generic.storage import Storage
from diagrams.generic.database import SQL
from diagrams.onprem.client import User
from diagrams.programming.language import Python
from diagrams.saas.chat import Slack
import os

os.environ["PATH"] += r";C:\Program Files\Graphviz\bin"
os.chdir(r"C:\Users\navin\ACO_Capstone\medicare-risk-ai\images")

with Diagram("Medicare ACO Risk Intelligence Platform", filename="architecture", outformat="png", show=False, direction="LR"):
    claims = Storage("Raw Medical\nClaims (96K lives)")
    snowflake = SQL("Snowflake\nIngestion Layer")
    analytics = SQL("Analytics\nFeature Engineering")
    ml = Python("Snowpark ML\nRandom Forest")
    cortex = SQL("Cortex AI\nLlama 3")
    app = User("Streamlit\nChatbot UI")

    claims >> snowflake >> analytics >> ml >> cortex >> app