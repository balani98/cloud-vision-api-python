import json
from google.auth import jwt
from google.cloud import pubsub_v1
import base64
def main(message,serviceaccountJson,project_id,topic_id):
	try:
		service_account_info = json.load(open(serviceaccountJson))
		audience = "https://pubsub.googleapis.com/google.pubsub.v1.Subscriber"

		credentials = jwt.Credentials.from_service_account_info(
			service_account_info, audience=audience
		)

		subscriber = pubsub_v1.SubscriberClient(credentials=credentials)

		# The same for the publisher, except that the "audience" claim needs to be adjusted
		publisher_audience = "https://pubsub.googleapis.com/google.pubsub.v1.Publisher"
		credentials_pub = credentials.with_claims(audience=publisher_audience)
		publisher = pubsub_v1.PublisherClient(credentials=credentials_pub)

		#topic_id='realtime_ism_topic'
		topic_path = publisher.topic_path(project_id, topic_id)
		#str_message=json.dumps(message)
		str_message=message

		
		encoded_message=str_message.encode("utf-8")
		#decoded_mesaage = base64.b64decode(encoded_message).decode('utf-8')

		#print(str(decoded_mesaage))
		publisher.publish(topic_path, encoded_message, spam='eggs')
		print("message passed to pub-sub topic")
	except Exception as e:
		print(str(e))