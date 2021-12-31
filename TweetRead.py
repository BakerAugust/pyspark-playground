'''
TweetRead.py

Stream tweets to a local port.

Adapted from https://www.udemy.com/course/spark-and-python-for-big-data-with-pyspark/
'''

from tweepy import Stream
import socket
import json
import os

# Load credentials
from dotenv import load_dotenv
load_dotenv()

# Set up your credentials
consumer_key=os.getenv('TWITTER_API_KEY')
consumer_secret=os.getenv('TWITTER_API_SECRET')
access_token =os.getenv('TWITTER_API_ACCESS_TOKEN')
access_secret=os.getenv('TWITTER_API_ACCESS_SECRET')


class TweetsListener(Stream):

  def __init__(
      self, 
      c_socket, 
      consumer_key, 
      consumer_secret, 
      access_token, 
      access_secret
  ):
        Stream.__init__(
            self, 
            consumer_key, 
            consumer_secret, 
            access_token, 
            access_secret
        )
        self.client_socket = c_socket

  def on_data(self, data):
      try:
          msg = json.loads( data )
          print( msg['text'].encode('utf-8') )
          self.client_socket.send( msg['text'].encode('utf-8') )
          return True
      except BaseException as e:
          print("Error on_data: %s" % str(e))
      return True

  def on_error(self, status):
      print(status)
      return True

def sendData(c_socket):
  twitter_stream = TweetsListener(
      c_socket, 
      consumer_key,
      consumer_secret, 
      access_token,
      access_secret
  )
  twitter_stream.filter(track=['agriculture'])

if __name__ == "__main__":
  s = socket.socket()         # Create a socket object
  host = "127.0.0.1"          # Get local machine name
  port = 5555                 # Reserve a port for your service.
  s.bind((host, port))        # Bind to the port

  print("Listening on port: %s" % str(port))

  s.listen(5)                 # Now wait for client connection.
  c, addr = s.accept()        # Establish connection with client.
    
  print( "Received request from: " + str( addr ) )

  sendData( c )