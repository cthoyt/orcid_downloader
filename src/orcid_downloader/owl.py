"""Write OWL."""

import gzip

import pyobo
from tqdm import tqdm

from orcid_downloader.api import VersionInfo, _get_output_module, iter_records

__all__ = [
    "write_owl_rdf",
]

PREAMBLE = """\
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix terms: <http://purl.org/dc/terms/> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix oboInOwl: <http://www.geneontology.org/formats/oboInOwl#> .
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .

@prefix o: <https://orcid.org/> .
@prefix r: <https://ror.org/> .
@prefix h: <http://purl.obolibrary.org/obo/NCBITaxon_9606> .
@prefix g: <http://purl.obolibrary.org/obo/OBI_0000245> .
@prefix s: <http://www.geneontology.org/formats/oboInOwl#hasExactSynonym> .
@prefix x: <http://www.w3.org/2004/02/skos/core#exactMatch> .
@prefix l: <http://www.w3.org/2000/01/rdf-schema#label> .
@prefix m: <http://www.w3.org/ns/org#memberOf> .
@prefix w: <https://schema.org/worksFor> .
@prefix e: <https://schema.org/alumniOf> .
@prefix hp: <http://xmlns.com/foaf/0.1/homepage> .
@prefix mb: <http://xmlns.com/foaf/0.1/mbox> .
@prefix db: <http://xmlns.com/foaf/0.1/depicted_by> .

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

w: a owl:AnnotationProperty;
    rdfs:label "employed by"^^xsd:string .

e: a owl:AnnotationProperty;
    rdfs:label "educated at"^^xsd:string .

m: a owl:AnnotationProperty;
    rdfs:label "member of"^^xsd:string .

hp: a owl:AnnotationProperty;
    rdfs:label "homepage"^^xsd:string .

mb: a owl:AnnotationProperty;
    rdfs:label "email"^^xsd:string .

g: a owl:Class ;
    rdfs:label "Organization"^^xsd:string .

h: a owl:Class ;
    rdfs:label "Homo sapiens"^^xsd:string .

db: a owl:AnnotationProperty;
    rdfs:label "depicted by"^^xsd:string .
"""


def write_owl_rdf(*, version_info: VersionInfo | None = None, force: bool = False) -> None:  # noqa:C901
    """Write OWL RDF in a gzipped file."""
    module = _get_output_module(version_info)
    path = module.join(name="orcid.ttl.gz")

    tqdm.write(f"Writing OWL RDF to {path}")

    ror_id_to_name = {k: v.replace('"', '\\"') for k, v in pyobo.get_id_name_mapping("ror").items()}
    ror_written = set()

    with gzip.open(path, "wt") as file:
        file.write(PREAMBLE + "\n")
        for record in iter_records(desc="Writing OWL RDF", version_info=version_info, force=force):
            if not record.name:
                continue
            ror_parts = []
            parts = ["a h:", f'l: "{record.name}"']
            for alias in record.aliases:
                parts.append(f's: "{alias}"')
            for prefix, value in sorted(record.xrefs.items()):
                parts.append(f'x: "{prefix}:{value}"')
            if record.commons_image:
                parts.append(f"db: <{record.commons_image_url}>")
            for org in record.employments:
                if not org.ror:
                    continue
                if org.ror not in ror_written:
                    ror_parts.append(f'r:{org.ror} a g:; l: "{ror_id_to_name[org.ror]}" .')
                    ror_written.add(org.ror)
                parts.append(f"w: r:{org.ror}")
            for education_org in record.educations:
                if not education_org.ror:
                    continue
                if education_org.ror not in ror_written:
                    ror_parts.append(
                        f'r:{education_org.ror} a g:; l: "{ror_id_to_name[education_org.ror]}" .'
                    )
                    ror_written.add(education_org.ror)
                parts.append(f"e: r:{education_org.ror}")
            for member_org in record.educations:
                if not member_org.ror:
                    continue
                if member_org.ror not in ror_written:
                    ror_parts.append(
                        f'r:{member_org.ror} a g:; l: "{ror_id_to_name[member_org.ror]}" .'
                    )
                    ror_written.add(member_org.ror)
                parts.append(f"m: r:{member_org.ror}")
            if record.homepage:
                parts.append(f"hp: <{record.homepage}>")
            for part in ror_parts:
                file.write(f"{part}\n")
            file.write(f"o:{record.orcid} " + "; ".join(parts) + " .\n")


if __name__ == "__main__":
    write_owl_rdf()
