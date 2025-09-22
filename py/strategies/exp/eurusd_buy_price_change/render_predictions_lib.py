import tensorflow as tf
import gc
from tensorflow import keras
import data
import psycopg2
import datetime
from multiprocessing import Process

def create_schema():
    conn = psycopg2.connect(
        host="localhost",
        database="trd",
        user="trd")

    cur = conn.cursor()
    cur.execute('drop table if exists example_prediction')

    cur.execute('''
        create table if not exists example_prediction (
            checkpoint bigint,
            split text,
            epoch_min bigint,
            predictions real[]
        )
    ''')

    conn.commit()
    conn.close()

def render_for_split(split, batch_size, checkpoint, model):
    ds = data.create_ds(split, batch_size, for_prediction=True)
    conn2 = psycopg2.connect(
        host="localhost",
        database="trd",
        user="trd")
    cur2 = conn2.cursor()
    db_res = []
    print('Rendering predictions ' + str(datetime.datetime.now()))
    for (x, y) in ds:
        # predictions = model.predict(x, batch_size=batch_size, verbose=0)
        predictions = model.predict_on_batch(x)
        i = 0
        for p in predictions:
            epoch_min = tf.keras.backend.get_value(y[i]).item()
            db_res.append([checkpoint, split, epoch_min, p.tolist()])
            i += 1
    print('Storing predictions to Db:' + str(datetime.datetime.now()))
    for r in db_res:
        cur2.execute('''
                    insert into example_prediction(checkpoint, split, epoch_min, predictions)
                    values(%s, %s, %s, %s)
                ''', r)
    print('Rendering predictions completed: ' + str(datetime.datetime.now()))
    conn2.commit()
    conn2.close()
    gc.collect()

def render_predictions_for_checkpoint(model_path, batch_size, checkpoint):
    model = keras.models.load_model(model_path)
    render_for_split('test', batch_size, checkpoint, model)
    render_for_split('eval', model)
    keras.backend.clear_session()
    del model
    tf.compat.v1.reset_default_graph()
    gc.collect()

def render_predictions(model_path, batch_size, checkpoints):
    for i in range(checkpoints):
        print('rendering predictions for checkpoint #' + str(i+1) + " " + str(datetime.datetime.now()))
        # render_predictions_for_checkpoint(model_path + str(i+1), batch_size, i+1, test_ds, eval_ds)
        p = Process(target=render_predictions_for_checkpoint,
                    args=(model_path + str(i+1), batch_size, i+1))
        p.start()
        p.join()
