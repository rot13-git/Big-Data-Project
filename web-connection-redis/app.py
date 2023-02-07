from flask import Flask, render_template, Response
import redis
from flask import url_for

app = Flask(__name__)
r = redis.StrictRedis(host='127.0.0.1', port=6379, db=0)


def event_stream():
    pubsub = r.pubsub()
    pubsub.subscribe('tweetDisaster')
    for message in pubsub.listen():
        #print (message)
        yield 'data: %s\n\n' % message['data']

def analysis_stream():
    pubsub2= r.pubsub()
    pubsub2.subscribe('tweetAnalysisDisaster')
    for message in pubsub2.listen():
        #print (message)
        yield 'data: %s\n\n' % message['data']

@app.route('/')
def show_homepage():
    return render_template("dashboard.html")

@app.route('/tweets')
def show_tweets():
    return render_template("tweets.html")

@app.route('/analisi')
def show_analisi():
    return render_template("analisi.html")


@app.route('/mappa')
def show_test():
    return render_template("map.html")

@app.route('/stream')
def stream():
    return Response(event_stream(), mimetype="text/event-stream")


@app.route('/analysis_stream')
def receive_analysis():
    return Response(analysis_stream(), mimetype="text/event-stream")


if __name__ == '__main__':
    app.run(threaded=True,host='0.0.0.0')

