import inspect
from collections import defaultdict
import inspect

def build_transitive_instance_clusters(module, stop_class):
    """
    Build a mapping: concrete_class -> list of all transitive descendants
    that share the same VersionStructure lineage.
    """
    # 1. Alle dataclass subclasses van stop_class
    classes = [
        cls for name, cls in inspect.getmembers(module, inspect.isclass)
        if issubclass(cls, stop_class) and cls is not stop_class and hasattr(cls, "__dataclass_fields__")
    ]

    # 2. Alle concrete instanties (geen VersionStructure of Dummy)
    concrete_classes = [
        cls for cls in classes
        if not cls.__name__.endswith("VersionStructure") and not cls.__name__.endswith("Dummy")
    ]

    # 3. Vind hoogste VersionStructure voor elke concrete klasse
    highest_version_map = {}
    for cls in concrete_classes:
        for base in inspect.getmro(cls):
            if base.__name__.endswith("VersionStructure"):
                highest_version_map[cls] = base
                break

    # 4. Groep instanties op hun VersionStructure
    clusters = defaultdict(list)
    for cls, ver in highest_version_map.items():
        clusters[ver].append(cls)

    # 5. Maak inverse mapping: VersionStructure -> VersionStructure kinderen
    version_children = defaultdict(list)
    for ver, insts in clusters.items():
        for cls in insts:
            for base in inspect.getmro(cls):
                if base in clusters and base is not ver:
                    version_children[base].append(cls)

    # 6. Bouw transitive clusters
    transitive_clusters = defaultdict(set)

    def add_descendants(node, descendants):
        for child in version_children.get(node, []):
            if child not in descendants:
                descendants.add(child)
                add_descendants(child, descendants)

    for cls in concrete_classes:
        ver = highest_version_map.get(cls)
        descendants = set()
        add_descendants(ver, descendants)
        # verwijder zichzelf als die per ongeluk meegenomen wordt
        descendants.discard(cls)
        transitive_clusters[cls] = list(descendants)

    return transitive_clusters


if __name__ == "__main__":
    import domain.netex.model
    x = build_transitive_instance_clusters(domain.netex.model, domain.netex.model.DataManagedObjectStructure)
    print(x[domain.netex.model.JourneyPattern])
    print(x[domain.netex.model.ServiceJourneyPattern])
    print(x[domain.netex.model.FareScheduledStopPoint])
    print(x[domain.netex.model.ScheduledStopPoint])
    print(x[domain.netex.model.ActivationPoint])
    print(x[domain.netex.model.TimingPoint])