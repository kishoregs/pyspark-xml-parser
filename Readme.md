# QRDA XML Parser

A PySpark-based parser for QRDA (Quality Reporting Document Architecture) Category I XML files. This parser can handle both single-patient and multi-patient QRDA documents.

## Features

- Parses both single-patient and multi-patient QRDA Category I XML files
- Automatically detects document format
- Extracts patient demographics and clinical observations
- Provides formatted output with derived columns
- Generates basic statistical analysis
- Handles errors gracefully
- Supports HL7 standard QRDA format

## Prerequisites

- Python 3.7+
- Apache Spark 3.x
- Java 8+

## Installation

1. Install required packages:
bash
pip install pyspark



2. Make sure you have Java installed and JAVA_HOME environment variable set

## File Structure


├── qrda_parser.py # Main parser implementation
├── sample_qrda.xml # Sample single-patient QRDA file
└── sample_qrda_multi.xml # Sample multi-patient QRDA file


## Usage

1. Place your QRDA XML files in the same directory as the parser
2. Run the parser:
   
bash
python qrda_parser.py


## Input File Formats

The parser supports two QRDA XML formats:

1. Single-patient format:
   - Patient information under `recordTarget/patientRole/patient`
   - Observations under `component/structuredBody/component/section/entry/observation`

2. Multi-patient format:
   - Multiple patients under `component/structuredBody/component/section/entry/patientRole`
   - Each patient entry contains both demographics and observations

## Output

The parser generates:

1. Patient Information Table:
   - Full Name
   - Gender
   - Formatted Birth Date
   - Measurement Value
   - Measurement Unit

2. Statistical Analysis:
   - Gender distribution
   - Measurement summary statistics

## Schema Structure

The parser uses a flexible schema that accommodates both formats:
- Patient demographics (name, gender, birth date)
- Clinical observations (codes, values, units)
- Document metadata

## Error Handling

- Validates XML structure
- Reports parsing errors
- Graceful handling of missing data
- Proper Spark session management

## Performance Considerations

- Uses PySpark for scalable processing
- Efficient XML parsing with spark-xml
- Minimal memory footprint
- Suitable for large QRDA files

## Limitations

- Currently supports basic QRDA Category I elements
- Requires valid HL7 QRDA format
- XML namespace handling is basic

## Contributing

1. Fork the repository
2. Create your feature branch
3. Commit your changes
4. Push to the branch
5. Create a new Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- Uses the spark-xml package from Databricks
- Follows HL7 QRDA Category I standard
- Inspired by healthcare interoperability needs