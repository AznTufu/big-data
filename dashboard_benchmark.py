"""
Dashboard Streamlit - Comparaison Pandas vs Spark
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path

st.set_page_config(
    page_title="Benchmark Pandas vs Spark",
    page_icon="⚡",
    layout="wide"
)

st.title("⚡ Benchmark: Pandas vs PySpark")
st.markdown("---")

# Charger les résultats
results_file = "benchmark_results.csv"

if not Path(results_file).exists():
    st.error("❌ Aucun résultat de benchmark trouvé!")
    st.info("Exécutez d'abord: `python benchmark_simple.py`")
    st.stop()

df = pd.read_csv(results_file)
df['timestamp'] = pd.to_datetime(df['timestamp'])

# Métriques globales
st.header("📊 Vue d'ensemble")

col1, col2, col3 = st.columns(3)

with col1:
    total_runs = len(df)
    st.metric("Nombre de tests", total_runs)

with col2:
    pandas_wins = (df['winner'] == 'Pandas').sum()
    spark_wins = (df['winner'] == 'Spark').sum()
    st.metric("Pandas gagne", f"{pandas_wins}/{total_runs}")

with col3:
    avg_speedup = df['speedup'].mean()
    st.metric("Speedup moyen", f"{avg_speedup:.1f}x")

st.markdown("---")

# Dernier benchmark
st.header("🏆 Dernier Benchmark")

if len(df) > 0:
    last = df.iloc[-1]
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🐼 Pandas")
        st.metric("Temps d'exécution", f"{last['pandas_time']:.2f}s")
        st.metric("Throughput", f"{last['pandas_throughput']:,.0f} records/sec")
    
    with col2:
        st.subheader("⚡ Spark")
        st.metric("Temps d'exécution", f"{last['spark_time']:.2f}s")
        st.metric("Throughput", f"{last['spark_throughput']:,.0f} records/sec")
    
    # Gagnant
    winner_emoji = "🐼" if last['winner'] == 'Pandas' else "⚡"
    st.success(f"{winner_emoji} **{last['winner']}** est **{last['speedup']:.2f}x** plus rapide!")

st.markdown("---")

# Graphiques de comparaison
st.header("📈 Analyse Comparative")

# Préparer les données pour visualisation
df_melted = df.melt(
    id_vars=['timestamp'],
    value_vars=['pandas_time', 'spark_time'],
    var_name='Engine',
    value_name='Temps (secondes)'
)
df_melted['Engine'] = df_melted['Engine'].map({
    'pandas_time': 'Pandas',
    'spark_time': 'Spark'
})

# 1. Comparaison des temps d'exécution
col1, col2 = st.columns(2)

with col1:
    fig1 = go.Figure()
    
    fig1.add_trace(go.Bar(
        name='Pandas',
        x=df.index,
        y=df['pandas_time'],
        marker_color='#00bfa5',
        text=df['pandas_time'].round(2),
        textposition='outside'
    ))
    
    fig1.add_trace(go.Bar(
        name='Spark',
        x=df.index,
        y=df['spark_time'],
        marker_color='#ff6f00',
        text=df['spark_time'].round(2),
        textposition='outside'
    ))
    
    fig1.update_layout(
        title="Temps d'Exécution (secondes)",
        xaxis_title="Test #",
        yaxis_title="Temps (s)",
        barmode='group',
        height=400
    )
    
    st.plotly_chart(fig1, use_container_width=True)

with col2:
    # 2. Comparaison du throughput
    fig2 = go.Figure()
    
    fig2.add_trace(go.Bar(
        name='Pandas',
        x=df.index,
        y=df['pandas_throughput'],
        marker_color='#00bfa5',
        text=df['pandas_throughput'].round(0),
        textposition='outside'
    ))
    
    fig2.add_trace(go.Bar(
        name='Spark',
        x=df.index,
        y=df['spark_throughput'],
        marker_color='#ff6f00',
        text=df['spark_throughput'].round(0),
        textposition='outside'
    ))
    
    fig2.update_layout(
        title="Throughput (records/seconde)",
        xaxis_title="Test #",
        yaxis_title="Records/sec",
        barmode='group',
        height=400
    )
    
    st.plotly_chart(fig2, use_container_width=True)

# 3. Évolution du speedup
st.subheader("🚀 Évolution du Speedup")

fig3 = go.Figure()

fig3.add_trace(go.Scatter(
    x=df.index,
    y=df['speedup'],
    mode='lines+markers',
    name='Speedup',
    line=dict(color='#673ab7', width=3),
    marker=dict(size=10),
    fill='tozeroy',
    fillcolor='rgba(103, 58, 183, 0.1)'
))

fig3.add_hline(y=1, line_dash="dash", line_color="gray", 
               annotation_text="Équivalence", annotation_position="right")

fig3.update_layout(
    title=f"Facteur de Speedup - {df['winner'].iloc[-1]} vs Concurrent",
    xaxis_title="Test #",
    yaxis_title="Speedup (x fois plus rapide)",
    height=400
)

st.plotly_chart(fig3, use_container_width=True)

# 4. Pie chart - Distribution des victoires
if len(df) > 1:
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🎯 Distribution des Victoires")
        
        winner_counts = df['winner'].value_counts()
        
        fig4 = go.Figure(data=[go.Pie(
            labels=winner_counts.index,
            values=winner_counts.values,
            hole=0.4,
            marker=dict(colors=['#00bfa5', '#ff6f00']),
            textinfo='label+percent',
            textfont_size=14
        )])
        
        fig4.update_layout(
            title="Qui gagne le plus souvent?",
            height=400
        )
        
        st.plotly_chart(fig4, use_container_width=True)
    
    with col2:
        st.subheader("📊 Statistiques")
        
        stats_df = pd.DataFrame({
            'Métrique': [
                'Temps moyen Pandas',
                'Temps moyen Spark',
                'Throughput moyen Pandas',
                'Throughput moyen Spark',
                'Speedup moyen',
                'Speedup max'
            ],
            'Valeur': [
                f"{df['pandas_time'].mean():.2f}s",
                f"{df['spark_time'].mean():.2f}s",
                f"{df['pandas_throughput'].mean():,.0f} rec/s",
                f"{df['spark_throughput'].mean():,.0f} rec/s",
                f"{df['speedup'].mean():.2f}x",
                f"{df['speedup'].max():.2f}x"
            ]
        })
        
        st.dataframe(stats_df, hide_index=True, use_container_width=True)

st.markdown("---")

# Historique détaillé
st.header("📋 Historique Complet")

df_display = df.copy()
df_display['timestamp'] = df_display['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
df_display['pandas_time'] = df_display['pandas_time'].round(3)
df_display['spark_time'] = df_display['spark_time'].round(3)
df_display['speedup'] = df_display['speedup'].round(2)
df_display['pandas_throughput'] = df_display['pandas_throughput'].round(0)
df_display['spark_throughput'] = df_display['spark_throughput'].round(0)

st.dataframe(df_display, use_container_width=True, hide_index=True)

# Conclusions
st.markdown("---")
st.header("💡 Conclusions")

col1, col2 = st.columns(2)

with col1:
    st.success("""
    **✅ Pandas excelle pour:**
    - Petits à moyens datasets (< 1M lignes)
    - Prototypage rapide
    - Analyses interactives
    - Faible latence
    """)

with col2:
    st.info("""
    **⚡ Spark excelle pour:**
    - Gros volumes (> 10M lignes)
    - Traitement distribué
    - Clusters multi-nœuds
    - Parallélisme massif
    """)

st.warning("""
**⚠️ Note importante:** Le overhead de démarrage de Spark (~15 secondes) le rend moins performant 
sur de petits datasets en mode local. Pour bénéficier de Spark, utilisez des datasets de plusieurs 
millions de lignes ou un vrai cluster distribué.
""")
