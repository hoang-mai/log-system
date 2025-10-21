from time import sleep

from prometheus_client import start_http_server, Gauge
import re
from dotenv import load_dotenv
import os
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import yaml

# Tạo dictionary chứa metric động
gauges = {}

# Load environment variables from .env file
load_dotenv()
urlElasticsearch = os.getenv("URL_ELASTICSEARCH")
es = Elasticsearch([urlElasticsearch])


def read_logs_from_rules(file_path, rules_file='rule.yml', index='logs', batch_size=500):
    # Load rules from YAML
    with open(rules_file, 'r') as f:
        rules_config = yaml.safe_load(f)

    # Determine which rule to apply based on file name
    file_name = os.path.basename(file_path)
    applicable_rule = None

    for rule in rules_config['rules']:
        if rule['file-name'] in file_name:
            if applicable_rule is None or rule['priority'] < applicable_rule['priority']:
                applicable_rule = rule

    if not applicable_rule:
        print(f"No applicable rule found for file: {file_name}")
        return []

    print(f"Applying rule '{applicable_rule['name']}' to file: {file_name}")

    # Read the log file
    with open(file_path, 'r', encoding='utf-8') as f:
        data = f.read()

    # Get the file type from the rule
    file_type = applicable_rule['file-name'].lower()

    # Process each condition in the rule
    actions = []

    if file_type == 'ext':
        # Process for Elasticsearch
        for condition in applicable_rule['conditions']:
            if condition['operator'] == 'matches':
                pattern = re.compile(condition['regex'], re.DOTALL)

                for match in pattern.finditer(data):
                    doc = {
                        'file': file_path,
                        'rule_name': applicable_rule['name']
                    }

                    # Extract outputs based on regex groups
                    for output in condition['outputs']:
                        group_index = output['source']
                        doc[output['name']] = match.group(group_index).strip()

                    actions.append({
                        '_index': index,
                        '_source': doc
                    })

        # Bulk index to Elasticsearch if there are documents
        total = len(actions)
        if total == 0:
            print("No documents matched the rule pattern.")
            return []

        print(f"Found {total} matching log entries for Elasticsearch.")

        # Send in batches to Elasticsearch
        success = 0
        for i in range(0, total, batch_size):
            batch = actions[i:i + batch_size]
            try:
                ok, errors = bulk(es, batch, raise_on_error=False)
                success += ok
                if errors:
                    print(f"Batch starting at {i} had {len(errors)} errors.")
            except Exception as e:
                print(f"Bulk indexing failed for batch starting at {i}: {e}")

        print(f"Indexed {success}/{total} documents to index '{index}'.")
        return actions

    elif file_type == 'mon':
        # Process for Prometheus metrics
        for condition in applicable_rule['conditions']:
            if condition['operator'] == 'matches':
                pattern = re.compile(condition['regex'])
                for match in pattern.finditer(data):
                    extracted = {}

                    # Extract outputs based on regex groups
                    for output in condition['outputs']:
                        group_index = output['source']
                        try:
                            value = match.group(group_index).strip()
                            extracted[output['name']] = value
                        except Exception:
                            extracted[output['name']] = None

                    # Create Prometheus gauges from extracted data
                    # Assume metric_name, object_path, and number_value fields
                    metric_name_field = extracted.get('metric_name', '')
                    object_path = extracted.get('object_path', '')
                    number_value = extracted.get('number_value', '0')

                    if metric_name_field:
                        # Sanitize metric name for Prometheus
                        metric_name = metric_name_field.replace('\\:', '_').replace('.', '_').replace('-', '_').lower()

                        try:
                            value = float(number_value)
                        except ValueError:
                            value = 0.0

                        # Create gauge if not exists
                        if metric_name not in gauges:
                            gauges[metric_name] = Gauge(metric_name, f"Auto metric from log: {metric_name}", ['object'])

                        gauges[metric_name].labels(object=object_path).set(value)

                        actions.append({
                            'file': file_path,
                            'rule_name': applicable_rule['name'],
                            'metric': metric_name,
                            'object': object_path,
                            'value': value
                        })

        print(f"Exposed {len(actions)} metrics to Prometheus from file '{file_path}'.")
        return actions

    else:
        print(f"Unknown file-name type '{file_type}' in rule. No action taken.")
        return []



if __name__ == "__main__":
    start_http_server(8000)

    log_file_path = 'C:\\Users\\maian\\Downloads\\logs_metrics_way4\\logs_metrics_way4'

    # Iterate through all files in the directory
    if os.path.isdir(log_file_path):
        print(f"Processing all files in directory: {log_file_path}")
        for filename in os.listdir(log_file_path):
            file_full_path = os.path.join(log_file_path, filename)
            # Only process files (not directories)
            if os.path.isfile(file_full_path):
                print(f"\n{'='*60}")
                print(f"Processing file: {filename}")
                print(f"{'='*60}")
                try:
                    read_logs_from_rules(file_full_path)
                except Exception as e:
                    print(f"Error processing file {filename}: {e}")
        print(f"\n{'='*60}")
        print("All files processed.")
    else:
        # If it's a single file, process it directly
        print(f"Processing single file: {log_file_path}")
        read_logs_from_rules(log_file_path)

    # Keep the server running for Prometheus metrics
    print("\nPrometheus metrics server running on port 8000...")
    print("Press Ctrl+C to stop.")
    try:
        while True:
            sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down...")
