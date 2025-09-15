from confluent_kafka import Producer
import vnstock
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

config

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        logger.error('Message delivery failed: {}'.format(err))
    else:
        logger.info('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))
