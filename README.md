# DBLP to Gradoop File Format

This tool converts a DBLP XML file to Gradoop readable JSON files. 
To do so it uses the functionality of a github project which can be found under: https://github.com/ScaDS/dblp-parser

To use this tool you first have to install the appropriate version of the dblp-parser.

# Execution
This tool could be easily executed from any IDE and takes 5 parameters:
 1. Location of the DBLP .xml file
 2. The number of elements to parse ('0' if all elements should be parsed)
 3. Output: Location of Gradoop JSON Graph Head File
 4. Output: Location of Gradoop JSON Vertex File
 5. Output: Location of Gradoop JSON Edges File
