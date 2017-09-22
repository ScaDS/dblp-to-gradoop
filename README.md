# DBLP to Gradoop File Format

This tool converts a DBLP XML file to Gradoop readable JSON files. 
To do so it uses the functionality of a github project which can be found under: https://github.com/ScaDS/dblp-parser

To use this tool you first have to install the appropriate version of the dblp-parser.
Furthermore you might have to use the following VM argument: -DentityExpansionLimit=2500000

# Execution
dblp-to-gradoop supports several possibilities for graph models derived from the dblp.xml
 * SimpleGraph (Vertices: Authors, Publication Types; Edges: Authors to Publications)
 * CoAuthorGraph (Vertices: Authors, Publication Types; Edges: Authors to Authors (if they worked at same publication), Authors to Publications)
