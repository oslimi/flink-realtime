#!/usr/bin/env python3
"""
=============================================================================
Performance Comparison: Java vs Python Flink Processing
=============================================================================

This script reads fraud alerts from Kafka and compares processing duration
between Java and Python implementations.

Usage:
    python compare_java_python_performance.py
"""
import json
from kafka import KafkaConsumer
import plotly.graph_objects as go

# Configuration
KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "fraud-alerts-stateful"


def fetch_messages(max_messages=1000, timeout_ms=5000):
    """Fetch messages from Kafka topic."""
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset='earliest',
        consumer_timeout_ms=timeout_ms,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    messages = []
    for msg in consumer:
        messages.append(msg.value)
        if len(messages) >= max_messages:
            break

    consumer.close()
    return messages


def categorize_messages(messages):
    """Split messages by source (Java vs Python) based on comment field."""
    java_msgs = []
    python_msgs = []

    for msg in messages:
        comment = msg.get('comment', '').upper()
        event_time = msg.get('currentTransaction', {}).get('eventTime', 0)
        processing_duration = msg.get('processingDuration', 0)

        data_point = {
            'eventTime': event_time,
            'processingDuration': processing_duration,
            'alertId': msg.get('alertId', '')
        }

        if 'PYTHON' in comment:
            python_msgs.append(data_point)
        elif 'JAVA' in comment:
            java_msgs.append(data_point)

    # Sort by eventTime
    java_msgs.sort(key=lambda x: x['eventTime'])
    python_msgs.sort(key=lambda x: x['eventTime'])

    return java_msgs, python_msgs


def moving_average(data, window=10):
    """Calculate moving average for trend visualization."""
    if len(data) < window:
        return data
    result = []
    for i in range(len(data)):
        start = max(0, i - window + 1)
        result.append(sum(data[start:i+1]) / (i - start + 1))
    return result


def plot_comparison(java_msgs, python_msgs):
    """Plot processing duration comparison using Plotly."""
    fig = go.Figure()

    # Java traces
    if java_msgs:
        java_durations = [m['processingDuration'] for m in java_msgs]
        java_avg = sum(java_durations) / len(java_durations)

        # Raw data points (semi-transparent)
        fig.add_trace(go.Scatter(
            x=list(range(len(java_msgs))),
            y=java_durations,
            mode='markers',
            name='Java (raw)',
            marker=dict(color='blue', size=5, opacity=0.3)
        ))

        # Moving average trend line
        fig.add_trace(go.Scatter(
            x=list(range(len(java_msgs))),
            y=moving_average(java_durations),
            mode='lines',
            name=f'Java trend (avg: {java_avg:.1f}ms)',
            line=dict(color='blue', width=3)
        ))

        # Average horizontal line
        fig.add_hline(y=java_avg, line_dash="dash", line_color="blue",
                      annotation_text=f"Java avg: {java_avg:.1f}ms",
                      annotation_position="left")

    # Python traces
    if python_msgs:
        python_durations = [m['processingDuration'] for m in python_msgs]
        python_avg = sum(python_durations) / len(python_durations)

        # Raw data points (semi-transparent)
        fig.add_trace(go.Scatter(
            x=list(range(len(python_msgs))),
            y=python_durations,
            mode='markers',
            name='Python (raw)',
            marker=dict(color='orange', size=5, opacity=0.3)
        ))

        # Moving average trend line
        fig.add_trace(go.Scatter(
            x=list(range(len(python_msgs))),
            y=moving_average(python_durations),
            mode='lines',
            name=f'Python trend (avg: {python_avg:.1f}ms)',
            line=dict(color='orange', width=3)
        ))

        # Average horizontal line
        fig.add_hline(y=python_avg, line_dash="dash", line_color="orange",
                      annotation_text=f"Python avg: {python_avg:.1f}ms",
                      annotation_position="right")

    fig.update_layout(
        title='<b>Flink Processing Duration: Java vs Python</b><br><sub>Trend lines show moving average, dashed lines show overall average</sub>',
        xaxis_title='Message Index (ordered by eventTime)',
        yaxis_title='Processing Duration (ms)',
        legend=dict(x=0.02, y=0.98, bgcolor='rgba(255,255,255,0.8)'),
        template='plotly_white',
        hovermode='x unified'
    )

    # Print stats
    print("\n📊 Performance Statistics:")
    print("-" * 50)
    if java_msgs:
        java_durations = [m['processingDuration'] for m in java_msgs]
        print(f"☕ Java:   count={len(java_msgs):4d}, avg={sum(java_durations)/len(java_durations):8.2f}ms, "
              f"min={min(java_durations):6d}ms, max={max(java_durations):6d}ms")
    if python_msgs:
        python_durations = [m['processingDuration'] for m in python_msgs]
        print(f"🐍 Python: count={len(python_msgs):4d}, avg={sum(python_durations)/len(python_durations):8.2f}ms, "
              f"min={min(python_durations):6d}ms, max={max(python_durations):6d}ms")

    if java_msgs and python_msgs:
        ratio = (sum(python_durations)/len(python_durations)) / (sum(java_durations)/len(java_durations))
        print("-" * 50)
        print(f"📈 Python/Java ratio: {ratio:.2f}x {'(Python slower)' if ratio > 1 else '(Java slower)'}")

    fig.show()


def main():
    print("🔍 Fetching messages from Kafka topic:", TOPIC)
    messages = fetch_messages()
    print(f"📨 Fetched {len(messages)} messages")

    java_msgs, python_msgs = categorize_messages(messages)
    print(f"☕ Java messages: {len(java_msgs)}")
    print(f"🐍 Python messages: {len(python_msgs)}")

    if not java_msgs and not python_msgs:
        print("❌ No messages found with JAVA or PYTHON in comment field")
        return

    plot_comparison(java_msgs, python_msgs)


if __name__ == "__main__":
    main()

