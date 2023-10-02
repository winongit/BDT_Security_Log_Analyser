from kafka import KafkaProducer
import random
import time

# Initialize Kafka producer
producer = KafkaProducer(bootstrap_servers=['192.168.1.66:9092'])

# List of usernames for simulation
usernames = ["user1", "user2", "admin", "john_doe", "jane_doe"]

# List of possible actions taken (e.g., "login", "logout", "access_denied", etc.)
actions = ["login", "logout", "access_denied"]

sourceIps = ["192.168.1.1", "192.168.1.5", "192.168.1.15","192.168.2.25", "192.168.3.5"]

def generate_security_log():
    username = random.choice(usernames)
    action = random.choice(actions)
    sourceIp = random.choice(sourceIps)
    timestamp = int(time.time())

    log = "{} User {} {} {}".format(timestamp, sourceIp, username, action)
    return log

if __name__ == "__main__":
    try:
        while True:
            log = generate_security_log()
            producer.send('security_logs_stream', value=log.encode('utf-8'))
            print("Sent: {}".format(log))
            time.sleep(random.uniform(1, 5))
    except Exception as e:
        print("Error: {}".format(e))
    finally:
        producer.flush()  # Ensure all messages are sent before exiting
