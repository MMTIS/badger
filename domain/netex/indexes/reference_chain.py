import inspect
from collections import defaultdict
import inspect

from collections import defaultdict
import inspect

def build_transitive_instance_clusters(module, stop_class):
    """
    Build a mapping: concrete_class -> list of all transitive descendants
    that share the same VersionStructure or ValueStructure lineage.
    """
    # 1️⃣ Alle dataclass subclasses van stop_class ophalen
    classes = [
        cls for name, cls in inspect.getmembers(module, inspect.isclass)
        if issubclass(cls, stop_class)
        and cls is not stop_class
        and hasattr(cls, "__dataclass_fields__")
    ]

    # 2️⃣ Alleen concrete instanties (geen *VersionStructure of *Dummy of *ValueStructure)
    concrete_classes = [
        cls for cls in classes
        if not (
            cls.__name__.endswith("VersionStructure")
            or cls.__name__.endswith("ValueStructure")
            or cls.__name__.endswith("Dummy")
        )
    ]

    # 3️⃣ Vind hoogste structure class (VersionStructure of ValueStructure)
    highest_structure_map = {}
    for cls in concrete_classes:
        for base in inspect.getmro(cls):
            name = base.__name__
            if name.endswith("VersionStructure") or name.endswith("ValueStructure"):
                highest_structure_map[cls] = base
                break

    # 4️⃣ Groepeer concrete klassen op hun bovenliggende structure type
    clusters = defaultdict(list)
    for cls, structure in highest_structure_map.items():
        clusters[structure].append(cls)

    # 5️⃣ Bouw mapping van structure -> afgeleide structures
    structure_children = defaultdict(list)
    for structure in clusters.keys():
        for other_structure in clusters.keys():
            if structure is not other_structure and issubclass(other_structure, structure):
                structure_children[structure].append(other_structure)

    # 6️⃣ Recursieve functie om alle transitieve descendants te verzamelen
    def collect_descendants(structure, collected):
        for child_structure in structure_children.get(structure, []):
            for inst in clusters.get(child_structure, []):
                if inst not in collected:
                    collected.add(inst)
            collect_descendants(child_structure, collected)

    # 7️⃣ Bouw eindmapping: concrete_class -> transitieve instanties
    transitive_clusters = defaultdict(list)
    for cls in concrete_classes:
        structure = highest_structure_map.get(cls)
        if not structure:
            continue
        descendants = set()
        collect_descendants(structure, descendants)
        # verwijder zichzelf als die per ongeluk voorkomt
        descendants.discard(cls)
        transitive_clusters[cls] = sorted(descendants, key=lambda c: c.__name__)

    return transitive_clusters

if __name__ == "__main__":
    import domain.netex.model
    x = build_transitive_instance_clusters(domain.netex.model,  domain.netex.model.DataManagedObjectStructure)
    print('JourneyPattern', x[domain.netex.model.JourneyPattern])
    print('ServiceJourneyPattern', x[domain.netex.model.ServiceJourneyPattern])
    print('FareScheduledStopPoint', x[domain.netex.model.FareScheduledStopPoint])
    print('ScheduledStopPoint', x[domain.netex.model.ScheduledStopPoint])
    print('ActivationPoint', x[domain.netex.model.ActivationPoint])
    print('TimingPoint', x[domain.netex.model.TimingPoint])
    print('Link', x[domain.netex.model.Link])
