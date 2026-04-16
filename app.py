from neo4j import GraphDatabase
from parse_ifc import parse_ifc  # your existing file

# --- CONFIG ---
NEO4J_URI      = "neo4j://127.0.0.1:7687"
NEO4J_USER     = "neo4j"
NEO4J_PASSWORD = "Amir44813141-"  # the password you set for NexusDatabase
IFC_PATH       = "C:\\Nextcloud\\Promotion\\NexusDatabase\\sample_data\\bimnexus.ifc"

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

# ── NODE CREATORS ──────────────────────────────────────────────

def upload_floors(tx, floors):
    for f in floors:
        tx.run("""
            MERGE (n:Floor {guid: $guid})
            SET n.name = $name, n.level = $level
        """, **f)

def upload_spaces(tx, spaces):
    for s in spaces:
        tx.run("""
            MERGE (n:Space {guid: $guid})
            SET n.name = $name, n.long_name = $long_name,
                n.area = $area, n.height = $height
        """, **s)

def upload_walls(tx, walls):
    for w in walls:
        tx.run("""
            MERGE (n:Wall {guid: $guid})
            SET n.name = $name, n.is_external = $is_external,
                n.material = $material, n.wall_type = $wall_type
        """, **w)

def upload_doors(tx, doors):
    for d in doors:
        tx.run("""
            MERGE (n:Door {guid: $guid})
            SET n.name = $name, n.width = $width,
                n.height = $height, n.area = $area
        """, **d)

def upload_windows(tx, windows):
    for w in windows:
        tx.run("""
            MERGE (n:Window {guid: $guid})
            SET n.name = $name, n.width = $width,
                n.height = $height, n.area = $area
        """, **w)

def upload_furniture(tx, furniture):
    for f in furniture:
        tx.run("""
            MERGE (n:Furniture {guid: $guid})
            SET n.name = $name, n.space_name = $space_name
        """, **f)

# ── RELATIONSHIP CREATORS ──────────────────────────────────────

def upload_wall_boundaries(tx, boundaries):
    for b in boundaries:
        tx.run("""
            MATCH (w:Wall  {guid: $wall_guid})
            MATCH (s:Space {name: $space_name})
            MERGE (w)-[r:BOUNDS]->(s)
            SET r.area = $area, r.length = $length,
                r.height = $height, r.wall_type = $wall_type
        """, **b)

def upload_door_boundaries(tx, boundaries):
    for b in boundaries:
        tx.run("""
            MATCH (d:Door  {guid: $door_guid})
            MATCH (s:Space {name: $space_name})
            MERGE (d)-[r:OPENS_INTO]->(s)
            SET r.area = $area, r.width = $width, r.height = $height
        """, **b)

def upload_window_boundaries(tx, boundaries):
    for b in boundaries:
        tx.run("""
            MATCH (w:Window {guid: $window_guid})
            MATCH (s:Space  {name: $space_name})
            MERGE (w)-[r:FACES]->(s)
            SET r.area = $area, r.width = $width, r.height = $height
        """, **b)

def upload_furniture_relations(tx, furniture):
    for f in furniture:
        if f["space_name"]:
            tx.run("""
                MATCH (item:Furniture {guid: $guid})
                MATCH (s:Space {name: $space_name})
                MERGE (item)-[:LOCATED_IN]->(s)
            """, **f)

# ── MAIN ──────────────────────────────────────────────────────

def main():
    print("📂 Parsing IFC file...")
    data = parse_ifc(IFC_PATH)

    with driver.session() as session:
        print("\n⬆️  Uploading nodes...")
        session.execute_write(upload_floors,    data["floors"])
        print(f"   ✅ {len(data['floors'])} Floors")

        session.execute_write(upload_spaces,    data["spaces"])
        print(f"   ✅ {len(data['spaces'])} Spaces")

        session.execute_write(upload_walls,     data["walls"])
        print(f"   ✅ {len(data['walls'])} Walls")

        session.execute_write(upload_doors,     data["doors"])
        print(f"   ✅ {len(data['doors'])} Doors")

        session.execute_write(upload_windows,   data["windows"])
        print(f"   ✅ {len(data['windows'])} Windows")

        session.execute_write(upload_furniture, data["furniture"])
        print(f"   ✅ {len(data['furniture'])} Furniture items")

        print("\n🔗 Creating relationships...")
        session.execute_write(upload_wall_boundaries,   data["boundaries"])
        print(f"   ✅ {len(data['boundaries'])} Wall-Space relationships")

        session.execute_write(upload_door_boundaries,   data["door_boundaries"])
        print(f"   ✅ {len(data['door_boundaries'])} Door-Space relationships")

        session.execute_write(upload_window_boundaries, data["window_boundaries"])
        print(f"   ✅ {len(data['window_boundaries'])} Window-Space relationships")

        session.execute_write(upload_furniture_relations, data["furniture"])
        print(f"   ✅ Furniture-Space relationships")

    driver.close()
    print("\n🎉 NexusDatabase is ready!")

if __name__ == "__main__":
    main()