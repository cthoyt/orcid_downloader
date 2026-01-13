"""Write OWL."""

from __future__ import annotations

import gzip
from itertools import chain
from pathlib import Path

import pyobo
import ssslm
from tqdm import tqdm

from orcid_downloader.api import VERSION_DEFAULT, VersionInfo, _get_output_module, iter_records

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

@prefix wikidata: <http://www.wikidata.org/entity/> .
@prefix scopus: <https://www.scopus.com/authid/detail.uri?authorId=> .
@prefix wos.researcher: <https://www.webofscience.com/wos/author/record/> .
@prefix sciprofiles: <https://sciprofiles.com/profile/> .
@prefix linkedin: <https://www.linkedin.com/in/> .
@prefix loop: <https://loop.frontiersin.org/people/> .
@prefix google.scholar: <https://scholar.google.com/citations?user=> .
@prefix researchgate.profile: <https://www.researchgate.net/profile/> .
@prefix cienciavitae: <https://www.cienciavitae.pt/> .
@prefix lattes: <https://lattes.cnpq.br/> .
@prefix github: <https://github.com/> .
@prefix kaken: <https://nrid.nii.ac.jp/ja/nrid/> .
@prefix gnd: <https://d-nb.info/gnd/> .
@prefix dialnet.author: <https://dialnet.unirioja.es/servlet/autor?codigo=> .
@prefix isni: <http://www.isni.org/isni/> .
@prefix authenticus: <https://www.authenticus.pt/> .
@prefix mastodon: <https://fedirect.toolforge.org/?id=> .
@prefix ssrn.author: <https://ssrn.com/author=> .
@prefix dblp.author: <https://dblp.org/pid/> .
@prefix osf: <https://osf.io/> .
@prefix publons.researcher: <https://publons.com/researcher/> .
@prefix ieee.author: <https://ieeexplore.ieee.org/author/> .
@prefix viaf: <http://viaf.org/viaf/> .
@prefix dockerhub.user: <https://hub.docker.com/u/> .

@prefix o: <https://orcid.org/> .
@prefix r: <https://ror.org/> .
@prefix pmid: <https://pubmed.ncbi.nlm.nih.gov/> .
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
@prefix k: <https://schema.org/keywords> .
@prefix p: <https://schema.org/author> .
@prefix ja: <http://purl.obolibrary.org/obo/IAO_0000013> .

<https://w3id.org/biopragmatics/resources/orcid.ttl> a owl:Ontology ;
    owl:versionInfo "{version}"^^xsd:string ;
    terms:title "ORCiD Instance Ontology" ;
    terms:description "An ontology representation of ORCiD" ;
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

k: a owl:AnnotationProperty;
    rdfs:label "has keyword"^^xsd:string .

p: a owl:AnnotationProperty;
    rdfs:label "author"^^xsd:string .

g: a owl:Class ;
    rdfs:label "Organization"^^xsd:string .

h: a owl:Class ;
    rdfs:label "Homo sapiens"^^xsd:string .

ja: a owl:Class ;
    rdfs:label "journal article"^^xsd:string .

db: a owl:AnnotationProperty;
    rdfs:label "depicted by"^^xsd:string .
"""


def write_owl_rdf(  # noqa:C901
    *,
    version_info: VersionInfo | None = None,
    force: bool = False,
    ror_grounder: ssslm.Grounder | None = None,
    ror_version: str | None = None,
) -> Path:
    """Write OWL RDF in a gzipped file."""
    if version_info is None:
        version_info = VERSION_DEFAULT
    module = _get_output_module(version_info)
    path = module.join(name="orcid.ttl.gz")

    tqdm.write(f"Writing OWL RDF to {path}")

    ror_id_to_name = pyobo.get_id_name_mapping("ror", version=ror_version)
    ror_written: set[str] = set()
    pmid_written: set[str] = set()

    with gzip.open(path, "wt") as file:
        file.write(PREAMBLE.format(version=version_info.version) + "\n")
        for record in iter_records(
            desc="Writing OWL RDF",
            version_info=version_info,
            force=force,
            ror_grounder=ror_grounder,
        ):
            if not record.name:
                continue
            ror_parts = []
            article_parts = []
            parts = ["a h:", f"l: {_escape(record.name)}"]
            for alias in record.aliases:
                parts.append(f"s: {_escape(alias)}")
            for prefix, value in sorted(record.xrefs.items()):
                if prefix == "mastodon":
                    continue
                elif _bad_luid(value):
                    tqdm.write(f"[orcid:{record.orcid}] had invalid xref: {prefix}:{value}")
                else:
                    parts.append(f"x: {prefix}:{value}")
            if record.commons_image:
                parts.append(f"db: <{record.commons_image_url}>")
            for org in record.employments:
                if not org.ror:
                    continue
                if org.ror not in ror_written:
                    if org_ror_name := ror_id_to_name.get(org.ror):
                        ror_parts.append(f"r:{org.ror} a g:; l: {_escape(org_ror_name)} .")
                    else:
                        ror_parts.append(f"r:{org.ror} a g: .")
                    ror_written.add(org.ror)
                parts.append(f"w: r:{org.ror}")
            for education_org in record.educations:
                if not education_org.ror:
                    continue
                if education_org.ror not in ror_written:
                    if education_org_ror_name := ror_id_to_name.get(education_org.ror):
                        ror_parts.append(
                            f"r:{education_org.ror} a g:; l: {_escape(education_org_ror_name)} ."
                        )
                    else:
                        ror_parts.append(f"r:{education_org.ror} a g: .")
                    ror_written.add(education_org.ror)
                parts.append(f"e: r:{education_org.ror}")
            for member_org in record.educations:
                if not member_org.ror:
                    continue
                if member_org.ror not in ror_written:
                    if member_org_ror_name := ror_id_to_name.get(member_org.ror):
                        ror_parts.append(
                            f"r:{member_org.ror} a g:; l: {_escape(member_org_ror_name)} ."
                        )
                    else:
                        ror_parts.append(f"r:{member_org.ror} a g: .")
                    ror_written.add(member_org.ror)
                parts.append(f"m: r:{member_org.ror}")
            if record.homepage:
                parts.append(f"hp: <{record.homepage}>")
            for keyword in sorted(record.keywords):
                parts.append(f"k: {_escape(keyword)}")
            for work in record.works:
                if work.pubmed not in pmid_written:
                    if work.title:
                        article_parts.append(
                            f"pmid:{work.pubmed} a ja:; l: {_escape(work.title)} ."
                        )
                    else:
                        article_parts.append(f"pmid:{work.pubmed} a ja:.")
                    pmid_written.add(work.pubmed)
                parts.append(f"p: pmid:{work.pubmed}")
            for part in chain(ror_parts, article_parts):
                file.write(part + "\n")
            file.write(f"o:{record.orcid} " + "; ".join(parts) + " .\n")

    return path


def _bad_luid(value: str) -> bool:
    # TODO more idiomatic checking
    return any(x in value for x in "?/&") or value.startswith("-")


def _escape(s: str) -> str:
    return '"' + s.replace('"', r"\"") + '"'


if __name__ == "__main__":
    write_owl_rdf()
