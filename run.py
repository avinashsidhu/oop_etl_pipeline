"""
Running the application
"""

import logging
import logging.config
import yaml
from os import getcwd

def main():
    """
    Entry point to run the ETL pipeline
    """
    # Parsing YAML
    config_path = f'{getcwd()}/configs/report1_config.yml'
    config = yaml.safe_load(open(config_path))
    # Configure logging
    log_config = config['logging']
    logging.config.dictConfig(log_config)
    logger = logging.getLogger(__name__)

if __name__ == '__main__':
    main()