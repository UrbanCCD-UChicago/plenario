from os.path import isfile
import datetime, time, random, sys
import boto.sqs as sqs
from os import environ
get = environ.get
AWS_ACCESS_KEY = get("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = get("AWS_SECRET_ACCESS_KEY")

conn = boto.sqs.connect_to_region(
        "us-east-1",
        aws_access_key_id = AWS_ACCESS_KEY_ID,
        aws_secret_access_key = AWS_SECRET_ACCESS_KEY
)

q = conn.create_queue("hyperunique-queue")

from boto.sqs.message import RawMessage
while not isfile("/var/tmp/terminate_runloop"):
	m = RawMessage()
	m.set_body('I am sender #{}.'.format(sys.argv[1]))
	retval = q.write(m)
	print 'added message, got retval: %s' % retval
	file = open("/var/log/sender.log", "a")
	file.write("I am sender #{}. The time is now {}.\n".format(sys.argv[1], datetime.datetime.now()))
	file.close()
	time.sleep(5)
file = open("/var/log/sender.log", "a")
file.write("**** I am sender \#{}. It's time to go! {}\n".format(sys.argv[1], datetime.datetime.now()))
time.sleep(random.randrange(10))
file.write("Goodbye {}.".format(sys.argv[1]))
file.close()

