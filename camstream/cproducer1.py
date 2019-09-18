import sys
import time
import cv2
from kafka import KafkaProducer
import numpy as np


fourcc = cv2.VideoWriter_fourcc(*'DIVX')
out = cv2.VideoWriter('motiondetect.avi',fourcc, 60.0, (1920,1080), isColor = False)

#foreground = cv2.createBackgroundSubtractorMOG2(detectShadows = False)

topic = "kafka_video"

def publish_video(video_file):

	producer = KafkaProducer(bootstrap_servers='localhost:9092')

	cap = cv2.VideoCapture(video_file)

	print("recording..")

	while(cap.isOpened()):

		success, camframe = cap.read()

		if not success:

			print("bad read")

			break

		
		ret, buffer = cv2.imencode('.jpg', camframe)

		print(1)

		print(type(buffer))

		producer.send(topic, buffer.tobytes())


		time.sleep(0.2)

	cap.release()

	print('end of stream')

def publish_camera():


	producer = KafkaProducer(bootstrap_servers='localhost:9092')

	camera = cv2.VideoCapture(0)

	camera.set(cv2.CAP_PROP_FRAME_WIDTH, 1920)
 	
	camera.set(cv2.CAP_PROP_FRAME_HEIGHT, 1080)

	camera.set(cv2.CAP_PROP_FPS, 6)

	try:

		while(True):

			success, camframe = camera.read()

			grayframe = camframe

			#grayframe = cv2.cvtColor(camframe, cv2.COLOR_BGR2GRAY)

			#blurframe = cv2.GaussianBlur(camframe, (5, 5), 0)

			#motionframe = foreground.apply(grayframe)

			detect = True

			if detect:

				print("record")

				out.write(grayframe)

			ret, buffer = cv2.imencode('.jpg', grayframe)

			print(type(buffer))





			producer.send(topic, buffer.tobytes())

			time.sleep(0.2)

	except:

		print("\n We are done.")

		sys.exit(1)

	camera.released()

	out.release()

if __name__ == '__main__':

	if(len(sys.argv) > 1):

		video_path = sys.argv[1]

		publish_video(video_path)

	else:
		
		print("We are live")

publish_camera()


	




















