import os
from dotenv import load_dotenv
load_dotenv(r"C:\Nextcloud\Promotion\NexusDatabase\.env")

from langchain_neo4j import GraphCypherQAChain, Neo4jGraph
from langchain_anthropic import ChatAnthropic
from langchain_core.prompts import PromptTemplate
import streamlit as st

# ── Custom Cypher generation prompt ─────────────────────────────────────────
# This tells Claude exactly how the schema works so it generates correct Cypher.
# The key instruction: always use Segment for per-room area, never Wall.area.

CYPHER_GENERATION_TEMPLATE = """
You are a Neo4j Cypher expert for a BIM building database.
Generate a Cypher query to answer the user's question.

SCHEMA:
{schema}

NODES:
- Floor     {{guid, name, level}}
- Space     {{guid, name, long_name, area, height}}
- Wall      {{guid, name, is_external, load_bearing, wall_type, material,
             length, height, width, area, u_value}}
- Segment   {{guid, wall_guid, wall_type, area, length, height,
             start_x, start_y, end_x, end_y, segment_index}}
- Slab      {{guid, name, slab_type, material, area, thickness}}
- Door      {{guid, name, width, height, area}}
- Window    {{guid, name, width, height, area}}
- Furniture {{guid, name, space_name}}

RELATIONSHIPS:
- (Floor)-[:CONTAINS_SPACE]  ->(Space)
- (Floor)-[:CONTAINS_WALL]   ->(Wall)
- (Floor)-[:CONTAINS_DOOR]   ->(Door)
- (Floor)-[:CONTAINS_WINDOW] ->(Window)
- (Wall) -[:HAS_SEGMENT]     ->(Segment)
- (Wall) -[:BOUNDS]          ->(Space)
- (Segment)-[:FACES]         ->(Space)
- (Wall) -[:HOSTS]           ->(Window)
- (Wall) -[:HOSTS]           ->(Door)
- (Door) -[:OPENS_INTO]      ->(Space)
- (Window)-[:FACES]          ->(Space)
- (Slab) -[:COVERS]          ->(Space)
- (Space)-[:HAS_FURNITURE]   ->(Furniture)

CRITICAL RULES — READ CAREFULLY:
1. For wall area PER ROOM (Raumbuch, painting, tiling):
   ALWAYS use: (Wall)-[:HAS_SEGMENT]->(Segment)-[:FACES]->(Space)
   Use Segment.area — this is the exact face area touching that room.

2. For total wall material (QTO, ordering):
   Use Wall.area directly — this is the full wall regardless of rooms.

3. NEVER use Wall.area when asked about area in a specific room.
   Wall.area = total wall area (both sides, full length).
   Segment.area = only the face touching that specific room.

4. Space long_name values: 'Bed room', 'Livingroom', 'Kitchen', 'Bathroom', 'WC'
5. wall_type values: 'ceramic', 'glass', 'drywall', 'structural', 'general'
6. is_external: true = exterior wall, false = interior wall

EXAMPLE QUERIES:

Q: What walls are connected to the Bathroom and what is their area?
A: MATCH (w:Wall)-[:HAS_SEGMENT]->(seg:Segment)-[:FACES]->(s:Space {{long_name:'Bathroom'}})
   RETURN w.name AS wall, w.wall_type AS type, SUM(seg.area) AS area_m2
   ORDER BY w.wall_type

Q: What is the ceramic tile area in the Bathroom?
A: MATCH (w:Wall {{wall_type:'ceramic'}})-[:HAS_SEGMENT]->(seg:Segment)-[:FACES]->(s:Space {{long_name:'Bathroom'}})
   RETURN SUM(seg.area) AS ceramic_area_m2

Q: What is the painting area in each room?
A: MATCH (w:Wall)-[:HAS_SEGMENT]->(seg:Segment)-[:FACES]->(s:Space)
   WHERE w.wall_type IN ['drywall','structural','general']
   RETURN s.long_name AS room, SUM(seg.area) AS painting_area_m2
   ORDER BY s.long_name

Q: Which wall hosts which window?
A: MATCH (w:Wall)-[:HOSTS]->(win:Window)
   RETURN w.name AS wall, win.name AS window

Q: Total drywall material needed (QTO)?
A: MATCH (w:Wall {{wall_type:'drywall'}})
   RETURN w.name, w.area AS total_area_m2

Q: How many rooms are there?
A: MATCH (s:Space) RETURN COUNT(s) AS total_rooms

Q: What is the floor area of each room?
A: MATCH (s:Space) RETURN s.long_name AS room, s.area AS floor_area_m2

Q: Which walls are external?
A: MATCH (w:Wall) WHERE w.is_external = true RETURN w.name, w.wall_type

Q: What furniture is in the bedroom?
A: MATCH (s:Space {{long_name:'Bed room'}})-[:HAS_FURNITURE]->(f:Furniture)
   RETURN f.name

Q: Which walls touch both Bathroom and WC?
A: MATCH (w:Wall)-[:HAS_SEGMENT]->(s1:Segment)-[:FACES]->(sp1:Space {{long_name:'Bathroom'}})
   MATCH (w)-[:HAS_SEGMENT]->(s2:Segment)-[:FACES]->(sp2:Space {{long_name:'WC'}})
   RETURN w.name

Q: Total wall area in WC?
A: MATCH (w:Wall)-[:HAS_SEGMENT]->(seg:Segment)-[:FACES]->(s:Space {{long_name:'WC'}})
   RETURN w.wall_type AS type, SUM(seg.area) AS area_m2

Question: {question}

Return ONLY the Cypher query. No explanation, no markdown, no backticks.
"""

CYPHER_GENERATION_PROMPT = PromptTemplate(
    input_variables=["schema", "question"],
    template=CYPHER_GENERATION_TEMPLATE,
)

# ── Setup (cached) ───────────────────────────────────────────────────────────
@st.cache_resource
def init_chain():
    graph = Neo4jGraph(
        url="neo4j://127.0.0.1:7687",
        username="neo4j",
        password="Amir44813141-",
    )
    graph.refresh_schema()

    llm = ChatAnthropic(
        model="claude-opus-4-5",
        temperature=0,
        api_key=os.getenv("ANTHROPIC_API_KEY"),
    )

    chain = GraphCypherQAChain.from_llm(
        llm=llm,
        graph=graph,
        verbose=True,
        allow_dangerous_requests=True,
        cypher_prompt=CYPHER_GENERATION_PROMPT,   # ← KEY FIX
        return_intermediate_steps=True,            # ← shows generated Cypher
    )
    return chain

# ── UI ───────────────────────────────────────────────────────────────────────
st.title("🏗️ BIM Nexus Chatbot")
st.caption("Ask questions about your building in plain English")

if "messages" not in st.session_state:
    st.session_state.messages = []

for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])
        if msg.get("cypher"):
            with st.expander("Generated Cypher"):
                st.code(msg["cypher"], language="cypher")

if question := st.chat_input("e.g. What walls are connected to the Bathroom?"):
    st.session_state.messages.append({"role": "user", "content": question})
    with st.chat_message("user"):
        st.markdown(question)

    with st.chat_message("assistant"):
        with st.spinner("Querying your building graph..."):
            chain  = init_chain()
            result = chain.invoke({"query": question})
            answer = result["result"]

            # Extract generated Cypher for debug display
            cypher = ""
            if "intermediate_steps" in result:
                for step in result["intermediate_steps"]:
                    if isinstance(step, dict) and "query" in step:
                        cypher = step["query"]
                        break

            st.markdown(answer)
            if cypher:
                with st.expander("🔍 Generated Cypher (debug)"):
                    st.code(cypher, language="cypher")

    st.session_state.messages.append({
        "role":    "assistant",
        "content": answer,
        "cypher":  cypher,
    })
