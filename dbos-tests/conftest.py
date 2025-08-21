import pytest
import os
from dbos import DBOS

@pytest.fixture()
def reset_dbos():
    DBOS.destroy()
    config: DBOSConfig = {
        "name": "hydroshare_metadata_extraction",
        "database_url": os.environ.get("TESTING_DATABASE_URL"),
    }
    DBOS(config=config)
    DBOS.reset_system_database()
    DBOS.launch()