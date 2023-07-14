import logging

def get_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)  # Set the logging level

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Add a StreamHandler to output to console
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)  # Set the logging level
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    return logger
