# Python 3.12+: paramiko/pysftp expect collections.MutableMapping (moved to collections.abc)
import collections
import collections.abc
if not hasattr(collections, "MutableMapping"):
    collections.MutableMapping = collections.abc.MutableMapping

from flask import Flask
from routes.gilbarco_counts import gilbarco_counts_bp
from routes.main import main_bp
from routes.fetch_data import fetch_data_bp

from routes.gilbarco_weekly import gilbarco_weekly_bp
from routes.gilbarco_itg_weekly import gilbarco_itg_weekly_bp
from routes.gilbarco_altria_scan import gilbarco_altria_scan_bp
from routes.gilbarco_itg_scan import gilbarco_itg_scan_bp
from routes.gilbarco_circana_scan import gilbarco_circana_scan_bp


app = Flask(__name__)

# Register Blueprints
app.register_blueprint(gilbarco_counts_bp)
app.register_blueprint(main_bp)
app.register_blueprint(fetch_data_bp)

app.register_blueprint(gilbarco_weekly_bp)
app.register_blueprint(gilbarco_itg_weekly_bp)
app.register_blueprint(gilbarco_altria_scan_bp)
app.register_blueprint(gilbarco_itg_scan_bp)
app.register_blueprint(gilbarco_circana_scan_bp)
if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000)