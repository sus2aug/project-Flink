# PyFlink Kinesis Table API Application

This project demonstrates how to create a **Table API Python Flink
application**.

------------------------------------------------------------------------

## Overview

-   The application reads data from a **Kinesis stream**:\
    `umarchine-kinesis-stream`

-   Records are processed and classified into two categories:

    -   ✅ **Good records** → Stored in
        `temperature_metrics_processed_records`
    -   ❌ **Bad records** → Stored in `temperature_metrics_bad_records`

------------------------------------------------------------------------

## References

-   📘 **Documentation**: [AWS Managed Flink - Python Create
    Application](https://docs.aws.amazon.com/managed-flink/latest/java/gs-python-createapp.html)\
-   💻 **GitHub Reference Code**:
    [amazon-managed-service-for-apache-flink-examples](https://github.com/aws-samples/amazon-managed-service-for-apache-flink-examples)
-   💻 **Make sure README.md is read before running this locally or on PROD**:

------------------------------------------------------------------------
