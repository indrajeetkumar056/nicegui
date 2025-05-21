from concurrent.futures import ThreadPoolExecutor
from kafka import KafkaConsumer, TopicPartition

from configs import *
from utils.helper import get_timestamp_and_uuid_from_message, get_start_of_day_timestamp, extract_products_from_buffer, create_dataframe_from_buffer
# from modules.synthetic_backfilling import *
# from modules.raw_ticks_correction_db import *
# from modules.raw_ticks_correction_druid import *


def automatic_backfill_function(AVAILABLE_SOURCE):
    print(AVAILABLE_SOURCE)
    executor = ThreadPoolExecutor(max_workers=8)
    print("Starting Kafka consumer...")

    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_BROKER,
        group_id=AUTOMATIC_BACKFILL_GROUP_ID,
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        value_deserializer=lambda v: v.decode('utf-8')
    )
    
    partitions = consumer.partitions_for_topic(AVAILABLE_SOURCE)
    if not partitions:
        print(f"No partitions found for topic {AVAILABLE_SOURCE}. Exiting.")
        return
    
    topic_partitions = [TopicPartition(AVAILABLE_SOURCE, p) for p in partitions]
    consumer.assign(topic_partitions) 

    # Fetch earliest offsets
    start_of_day_timestamp = get_start_of_day_timestamp()
    timestamps = {tp: start_of_day_timestamp for tp in topic_partitions}

    # Seek to the earliest offset available after start_of_day_timestamp
    offsets_for_times = consumer.offsets_for_times(timestamps)
    for tp, offset_data in offsets_for_times.items():
        if offset_data is not None:  # Offset exists
            consumer.seek(tp, offset_data.offset)
        else:
            consumer.seek_to_beginning(tp)

    buffers = {} 
    sub_buffers = {}
    processing_uuids = set()
    min_timestamps = {}

    try:
        for message in consumer:
            key = message.key.decode('utf-8') if message.key else None
            value = message.value

            if key == '@@start':
                print("Detected '@@start' key, beginning new chunk.")
                try:
                    min_timestamp, uuid = get_timestamp_and_uuid_from_message(value)
                    if uuid not in processing_uuids:
                        processing_uuids.add(uuid)
                        buffers.setdefault(uuid, [])  
                    else:
                        print(f"Warning: Duplicate start for uuid {uuid}, ignoring.")
                except Exception as e:
                    print(f"Error processing '@@start': {e}")

            elif key == '@@end':
                print("Detected '@@end' key, processing chunk...")
                try:
                    _, uuid = get_timestamp_and_uuid_from_message(value)
                    if uuid in processing_uuids:
                        processing_uuids.remove(uuid)
                        buffer = buffers.pop(uuid, [])
                        main_df = create_dataframe_from_buffer(buffer)
                        print(main_df)
                        # executor.submit(process_synthetic_chunk_async, uuid, main_df)
                    else:
                        print(f"Warning: '@@end' received for unknown uuid {uuid}, ignoring.")
                except Exception as e:
                    print(f"Error processing '@@end': {e}")    

            elif key == '@start':
                print("Detected '@start', beginning sub-session.")
                try:
                    min_timestamp, sub_uuid = get_timestamp_and_uuid_from_message(value)
                    main_uuid = sub_uuid.split('.')[0]  
                    if main_uuid in processing_uuids:
                        sub_buffers.setdefault(sub_uuid, [])
                        min_timestamps[sub_uuid] = min_timestamp
                    else:
                        print(f"Warning: Sub-session {sub_uuid} without corresponding main session {main_uuid}, ignoring.")
                except Exception as e:
                    print(f"Error processing '@start': {e}")
            
            elif key == '@end':
                print("Detected '@end', processing sub-session.")
                try:
                    max_timestamp, sub_uuid = get_timestamp_and_uuid_from_message(value)
                    if sub_uuid in sub_buffers:
                        buffer = sub_buffers.pop(sub_uuid, [])
                        min_timestamp = min_timestamps.pop(sub_uuid, None)
                        products = extract_products_from_buffer(buffer)
                        if min_timestamp is not None:
                            print(f"Submitting correction task for sub-session {sub_uuid}...")
                            sub_df = create_dataframe_from_buffer(buffer)
                            print(sub_df)
                            # executor.submit(raw_ticks_correction_in_db, sub_uuid, sub_df, products, min_timestamp, max_timestamp)
                        else:
                            print(f"Warning: No min timestamp found for sub-session {sub_uuid}.")
                    else:
                        print(f"Warning: '@end' encountered without corresponding '@start' for sub-session {sub_uuid}.")
                except Exception as e:
                    print(f"Error processing '@end': {e}")
            
            else:
                try:
                    min_timestamp, sub_uuid = get_timestamp_and_uuid_from_message(value)
                    main_uuid = sub_uuid.split('.')[0]
                    buffers.setdefault(main_uuid, []).append(value)  
                    sub_buffers.setdefault(sub_uuid, []).append(value)  
                except Exception as e:
                    print(f"Error processing message with key {key}: {e}")

    except Exception as e:
        print(f"Error while consuming messages: {e}")
        raise e
    
    finally:
        executor.shutdown(wait=True)
        consumer.close()


        