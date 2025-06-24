"""Standardization of academic degrees and organizational roles.

1. https://degree.studentnews.eu lists degrees conferred in the EU/Europe
2. https://github.com/vivo-ontologies/academic-degree-ontology is an incomplete/abandoned
   effort from 2020 to ontologize degree names
3. Wikidata has a class for academic degree https://www.wikidata.org/wiki/Q189533. Its
   `SPARQL query service <https://query.wikidata.org>`_ can be queried with the following,
   though note that the Wikidata class hierarchy is broken in several places.

   .. code-block:: sparql

      SELECT ?item ?itemLabel WHERE {
         ?item wdt:P279* wd:Q189533 .
         SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
       }
"""

from collections import Counter
from pathlib import Path

import pystow
import qualo
from curies import NamedReference
from tqdm import tqdm

__all__ = [
    "standardize_role",
]


def _norm(s: str) -> str:
    return (
        s.lower()
        .strip()
        .replace(".", "")
        .replace("_", "")
        .replace(" ", "")
        .replace("-", "")
        .replace("'", "")
        .replace("’", "")  # noqa:RUF001
    )


REVERSE_REPLACEMENTS = {
    "Diploma": {"High School Diploma", "High School"},
    "Intern": {"Internship", "intern", "Research Intern"},
    "Trainee": {"Trainee", "Estagiário", "Estagiária"},
    "Student": {"Estudiante", "студент", "Estudante"},
    "Professor": {
        "professor",
        "Profesora",
        "Professora",
        "Profesor",
        "Full Professor",
        "Prof.",
        "Prof. Dr.",
        "Research Professor",
        "Profesor Investigador",
        "Profesor Titular de Universidad",
        "профессор",
        "Professor Emeritus",
        "Professeur",
        "Emeritus Professor",
    },
    "Postdoctoral Researcher": {
        "Postdoctoral researcher",
        "postdoc",
        "postdoctoral research fellow",
        "Postdoctoral Research Associate",
        "Postdoctoral fellow",
        "Post-doc",
        "Postdoctoral Scholar",
        "Postdoctoral Associate",
        "Postgraduate",  # questionable
        "Postdoctoral",
        "Postdoctor",
        "Postdoc Researcher",
        "Postdoc researcher",
        "Postdoc Fellow",
        "Postdoctoral Scientist",
        "Post Doctoral Fellowship",
        "Postdoctoral Fellowship",
        "Postdoctorate",
        "Postdoctoral fellowship",
        "Postdoctoral Research",
        "Posdoc",
        "Postdoctorado",
        "Postdoctoral research",
        "Pós-Doutorado",
        "Pós-doutorado",
        "Post Doctorat",
    },
    "Medical Resident": {"residency", "resident"},
    "Nurse": {"Enfermagem", "Enfermeira", "Enfermera"},
    "Researcher": {
        "Research Fellow",
        "Research Scientist",
        "Senior Researcher",
        "Scientist",
        "Senior Research Fellow",
        "Senior Research Scientist",
        "Research Scholar",
        "Senior Scientist",
        "Senior Research Associate",
        "Visiting Researcher",
        "Junior Researcher",
        "Staff Scientist",
        "Principal Scientist",
        "Researcher (Academic)",
        "Assistant Researcher",
        "Ricercatori",
        "Research",
        "Research Specialist",
        "Wissenschaftlicher Mitarbeiter",
        "Scientific Researcher",
        "Graduate Student Researcher",  # TODO
        # Specific to field
        "Biologist",
        "Biology",
    },
    "Department Head": {"Head of the Department"},
    "Assistant Professor": {
        "Assistant professor",
        "Research Assistant Professor",
        "Profesor Asociado",
        "Asst. Professor",
        "Adjunct Assistant Professor",  # or is this adjunct?
    },
    "Associate Professor": {
        "Associate professor",
        "Profesor Titular",
        "Associated Professor",
        "Assoc. Prof.",
        "Professor Associado",
    },
    "Adjunct Professor": {"Professor Adjunto", "Professora Adjunta"},
    "Teaching Assistant": {"Teaching Assistant", "Graduate Teaching Assistant"},
    "Research Assistant": {
        "Research assistant",
        "Graduate Research Assistant",
        "Undergraduate Research Assistant",
    },
    "Research Associate": {"Research associate"},
    "Docent": {"доцент", "Docenti di ruolo di Ia fascia", "DOCENTE"},
    "Lecturer": {
        "lecturer",
        "Instructor",
        "Mestre",
        "Teacher",
        "Lecture",
        "Senior lecturer",
        "Associate Lecturer",
        "старший преподаватель",
        "Adjunct Lecturer",
    },
    "Assistant Lecturer": {"Assistant Lecturer"},
    "Psychologist": {"Psicólogo", "Psicóloga", "Psicologia"},
    "Physiotherapist": {"Fisioterapeuta"},
    "Lawyer": {"Abogado", "Abogada"},
    "Software Developer": {"Software Developer", "Software Engineer"},
    "Specialist": {
        "Especialização",
        "Especialista",
        "специалист",
    },
    "Engineer": {
        "Chemical Engineer",
        "Engineer",
        "Ingeniero Industrial",
        "Ing.",
        "Ingeniero Civil",
        "Ingeniero Agrónomo",
        "Mechanical Engineer",
        "Civil Engineer",
        "Ingeniero de Sistemas",
        "engineer",
        "Chemical Engineering",
        "Ingeniero Químico",
        "Engenheiro Agrônomo",
        "Engenharia Civil",
        "Mechanical Engineering",
        "Electrical Engineer",
        "Ingeniero Electrónico",
        "Ingeniero Mecánico",
        "Industrial Engineer",
        "Civil Engineering",
        "Computer Engineering",
        "Electronic Engineer",
    },
}
for k in REVERSE_REPLACEMENTS:
    REVERSE_REPLACEMENTS[k].add(_norm(k))

REPLACEMENTS = {_norm(value): k for k, values in REVERSE_REPLACEMENTS.items() for value in values}

#: contains all roles
ROLE_COUNTER_1: Counter[str] = Counter()

#: contains roles that should be immediately curated as part of qualo
ROLE_COUNTER_2: Counter[str] = Counter()


def standardize_role(role: str) -> tuple[str, bool, NamedReference | None]:  # noqa: C901
    """Standardize a role string."""
    role = role.strip().replace("\t", " ").replace("  ", " ")

    role = role.removeprefix("Visiting ")
    role = role.removeprefix("Junior ")
    role = role.removeprefix("Senior ")

    role = role.strip()

    reference = qualo.ground(role)
    if reference:
        return reference.name, True, reference

    role_norm = _norm(role)
    if role_norm in REPLACEMENTS:
        return REPLACEMENTS[role_norm], True, None

    ROLE_COUNTER_1[role] += 1

    for splits in [" in ", " of "]:
        if splits not in role:
            continue
        x = role.split(splits, 2)[0]
        reference = qualo.ground(x)
        if reference:
            # TODO get rid of this - everything in here should get curated
            ROLE_COUNTER_2[role] += 1
            return reference.name, True, reference
        y = _norm(x)
        if y in REPLACEMENTS:
            return REPLACEMENTS[y], True, None

    if role_norm.startswith("bscin") or role_norm.startswith("bsc "):
        # TODO get rid of this - everything in here should get curated
        ROLE_COUNTER_2[role] += 1
        return "Bachelor of Science", True, None
    if role_norm.startswith("mscin") or role_norm.startswith("msc "):
        # TODO get rid of this - everything in here should get curated
        ROLE_COUNTER_2[role] += 1
        return "Master of Science", True, None
    if role_norm.startswith("main") or role_norm.startswith("ma "):
        # TODO get rid of this - everything in here should get curated
        ROLE_COUNTER_2[role] += 1
        return "Master of Arts", True, None
    if role_norm.startswith("phdin") or role_norm.startswith("phd student in "):
        # TODO get rid of this - everything in here should get curated
        ROLE_COUNTER_2[role] += 1
        return "Doctor of Philosophy", True, None

    return role, False, None


def write_role_counters() -> None:
    """Write summaries over all roles,a nd the highest."""
    write_counter(ROLE_COUNTER_1, pystow.join("orcid", name="roles_all.tsv"))
    write_counter(ROLE_COUNTER_2, pystow.join("orcid", name="roles_curate_first.tsv"))


def write_counter(
    counter: Counter[str] | Counter[tuple[str, ...]],
    path: str | Path,
    sep: str | None = None,
    header=None,
) -> None:
    """Write a counter."""
    path = Path(path).expanduser().resolve()
    tqdm.write(f"Writing to {path}")
    if sep is None:
        sep = "\t"
    key_values = counter.most_common()
    if not key_values:
        return None
    with path.open("w") as file:
        if isinstance(key_values[0][0], tuple):
            if header is None:
                pp = (f"key_{i + 1}" for i in range(len(key_values[0][0])))
                header = (*pp, "count")
            print(*header, sep=sep, file=file)
            for key, value in key_values:
                print(*key, value, sep=sep, file=file)
        else:
            if header is None:
                header = "key", "count"
            print(*header, sep=sep, file=file)
            for key, value in key_values:
                print(key, value, sep=sep, file=file)
