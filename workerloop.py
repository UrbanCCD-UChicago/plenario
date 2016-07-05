from os.path import isfile
import datetime, time, random, sys
import boto.sqs
from plenario.settings import AWS_ACCESS_KEY, AWS_SECRET_KEY, REDIS_HOST

conn = boto.sqs.connect_to_region(
        "us-east-1",
        aws_access_key_id = AWS_ACCESS_KEY,
        aws_secret_access_key = AWS_SECRET_KEY
)

q = conn.get_queue("testy-queue")

while not isfile("/var/tmp/terminate_runloop"):
	rs = q.get_messages(message_attributes=["ticket"])
	file = open("/opt/python/log/worker.log", "a")
	if len(rs) > 0:
		try:
			ticket = rs[0].message_attributes["ticket"]["string_value"]
		except:
			file.write("Message does not contain a ticket! Skipping.\n")
			file.close()
			continue
		body = rs[0].get_body()
		
		import redis
		pool = redis.ConnectionPool(host=REDIS_HOST, port=6379, db=0)
		r = redis.Redis(connection_pool=pool)
		result = r.get("query_"+ticket)
		r.set("result_"+ticket, result)
		
		file.write("I am worker #{}. The time is now {}. I got a message: {}. It had a ticket! \"{}\" The result was {}.\n".format(sys.argv[1], datetime.datetime.now(), body, ticket, result))
		
		q.delete_message(rs[0])
		
	else:
		file.write("I am worker #{}. The time is now {}. No tickets to read.\n".format(sys.argv[1], datetime.datetime.now()))
	file.close()
	time.sleep(5)
file = open("/opt/python/log/worker.log", "a")
file.write("**** I am worker \#{}. It's time to go! {}\n".format(sys.argv[1], datetime.datetime.now()))
time.sleep(random.randrange(10))
file.write("Goodbye {}.".format(sys.argv[1]))
file.close()

