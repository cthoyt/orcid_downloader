"""Write OWL."""

import gzip

from orcid_downloader.api import MODULE, iter_records

__all__ = [
    "write_owl_rdf",
]

PATH = MODULE.join(name="orcid.ttl.gz")

PREAMBLE = """\
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix terms: <http://purl.org/dc/terms/> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix oboInOwl: <http://www.geneontology.org/formats/oboInOwl#> .
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .

@prefix o: <https://orcid.org/> .
@prefix h: <http://purl.obolibrary.org/obo/NCBITaxon_9606> .
@prefix s: <http://www.geneontology.org/formats/oboInOwl#hasExactSynonym> .
@prefix x: <http://www.w3.org/2004/02/skos/core#exactMatch> .
@prefix l: <http://www.w3.org/2000/01/rdf-schema#label> .

<https://w3id.org/biopragmatics/resources/orcid.ttl> a owl:Ontology ;
    owl:versionInfo "2023"^^xsd:string ;
    terms:title "ORCID Instance Ontology" ;
    terms:description "An ontology representation of ORCID" ;
    terms:license <https://creativecommons.org/publicdomain/zero/1.0/> ;
    rdfs:comment "Built by https://github.com/cthoyt/orcid_downloader"^^xsd:string .

l: a owl:AnnotationProperty;
    rdfs:label "label"^^xsd:string .

s: a owl:AnnotationProperty;
    rdfs:label "has exact synonym"^^xsd:string .

x: a owl:AnnotationProperty;
    rdfs:label "exact match"^^xsd:string .

h: a owl:Class ;
    rdfs:label "Homo sapiens"^^xsd:string .
"""


def write_owl_rdf() -> None:
    """Write OWL RDF in a gzipped file."""
    with gzip.open(PATH, "wt") as file:
        file.write(PREAMBLE + "\n")
        for record in iter_records():
            if not record.name:
                continue
            parts = ["a h:", f'l: "{record.name}"']
            for alias in record.aliases:
                parts.append(f's: "{alias}"')
            for prefix, value in sorted(record.xrefs.items()):
                parts.append(f'x: "{prefix}:{value}"')
            file.write(f"o:{record.orcid} " + "; ".join(parts) + " .\n")


if __name__ == "__main__":
    write_owl_rdf()
