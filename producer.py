from datetime import datetime
from json import dumps
from kafka import KafkaProducer
from random import choice, randrange
from sys import argv
from time import sleep

producer = KafkaProducer(
	bootstrap_servers=["localhost:9092"],
	value_serializer=lambda x: dumps(x).encode("utf-8")
)

"""for j in range(999):
   print("Iteration", j)
   data = {"counter": j}
   producer.send("student-activity", value=data)
   sleep(0.5)"""

# Event types
events = (
	"video_start",
	"quiz_submit",
	"Login"
)

# Number of events, students
iterations, students = int(argv[1]), int(argv[2])

for j in range(iterations):
	print(f"Iteration {j}")

	# Data to send
	data = {
		"timestamp" : datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
		"event_id" : j,
		"student_id" : randrange(0, students),
		"event_type" : choice(events)
	}

	producer.send("student_activity", value=data)
	sleep(0.5)

producer.flush()
producer.close()
