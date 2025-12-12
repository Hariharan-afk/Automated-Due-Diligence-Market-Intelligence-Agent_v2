"""
Initial Load DAG - One-time Load Historical historical Data

Fetches and processes data for all companies:
- SEC Filings (10-K, 10-Q) from 2023+
- Wikipedia pages
- News articles (last 30 days)

Schedule: Manual trigger only
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup

# Default arguments
default_args = {
    'owner': 'data_pipeline',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'initial_load',
    default_args=default_args,
    description='Initial historical data load for all companies',
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2024, 12, 1),
    catchup=False,
    tags=['initial', 'historical', 'all-companies'],
)


def load_companies(**context):
    """Load companies list from configuration"""
    import sys
    from pathlib import Path
    
    # Add project root to path
    project_root = Path(__file__).parent.parent.parent
    sys.path.insert(0, str(project_root))    
    
    from src.orchestration.utils.airflow_helpers import get_companies_list
    
    companies = get_companies_list()
    context['task_instance'].xcom_push(key='companies', value=companies)
    
    return f"Loaded {len(companies)} companies"


def process_company_data(ticker, company_name, cik, **context):
    """
    Process all data for a single company with comprehensive metadata
    
    Uses working connector-based architecture from test_apple_2024.py
    Includes 25+ metadata fields for enhanced RAG performance
    """
    import sys
    import os
    from datetime import datetime, timedelta
    from dotenv import load_dotenv
    
    # Setup paths for Docker container
    data_pipeline_path = '/opt/airflow/Data_Pipeline'
    if data_pipeline_path not in sys.path:
        sys.path.insert(0, data_pipeline_path)
    
    load_dotenv(dotenv_path='/opt/airflow/.env')
    
    # Import actual classes that exist
    from src.data_ingestion.sec_fetcher import SECFetcher
    from src.data_ingestion.wikipedia_fetcher import WikipediaFetcher
    from src.data_ingestion.news_fetcher import NewsFetcher
    from src.data_processing.chunker import TextChunker
    from src.data_processing.table_processor import TableProcessor
    from src.utils.table_summarizer import GroqTableSummarizer
    from src.embedding import Embedder
    from src.cloud.gcs_connector import GCSConnector
    from src.cloud.qdrant_connector import QdrantConnector
    
    print(f"\n{'='*80}")
    print(f"PROCESSING: {company_name} ({ticker})")
    print(f"{'='*80}\n")
    
    # Initialize components
    print("🔧 Initializing components...")
    
    gcs = GCSConnector(
        bucket_name=os.getenv('GCP_BUCKET_NAME'),
        project_id=os.getenv('GCP_PROJECT_ID'),
        credentials_path=os.getenv('GCP_CREDENTIALS_PATH')
    )
    
    qdrant = QdrantConnector(
        url=os.getenv('QDRANT_URL'),
        api_key=os.getenv('QDRANT_API_KEY')
    )
    
    embedder = Embedder()
    chunker = TextChunker(chunk_size=512, overlap=128)
    table_summarizer = GroqTableSummarizer(api_key=os.getenv('GROQ_API_KEY'))
    table_processor = TableProcessor(summarizer=table_summarizer, min_table_size=0)
    
    collection_name = os.getenv('QDRANT_COLLECTION', 'company_data')
    
    # Ensure collection exists
    try:
        qdrant.create_collection(
            collection_name=collection_name,
            vector_size=1024,
            distance='Cosine',
            recreate=False
        )
    except:
        pass  # Collection already exists
    
    stats = {
        'sec': {'filings': 0, 'chunks': 0, 'tables': 0},
        'wikipedia': {'pages': 0, 'chunks': 0},
        'news': {'articles': 0, 'chunks': 0}
    }
    
    # === 1. Process SEC Filings with Comprehensive Metadata ===
    print("\n📄 Processing SEC Filings...")
    try:
        sec_fetcher = SECFetcher(user_identity=os.getenv('SEC_API_KEY', 'your.email@example.com'))
        
        filings = sec_fetcher.fetch_filings_by_cik(
            cik=cik,
            filing_types=['10-K', '10-Q'],
            start_date='2023-01-01',
            end_date=datetime.now().strftime('%Y-%m-%d')
        )
        
        stats['sec']['filings'] = len(filings)
        print(f"   Found {len(filings)} filings")
        
        for filing in filings:
            print(f"   Processing {filing['filing_type']} - {filing['filing_date']}")
            
            for section in filing['sections']:
                # Process tables
                processed_text, tables = table_processor.process_section(
                    section_html=section.get('section_html_doc'),
                    section_text=section['section_text'],
                    metadata={
                        'ticker': ticker,
                        'filing_type': filing['filing_type'],
                        'section': section['section_code'],
                        'section_name': section['section_name']
                    }
                )
                
                stats['sec']['tables'] += len(tables)
                
                # Chunk text
                chunks = chunker.chunk_text(processed_text)
                if not chunks:
                    continue
                
                stats['sec']['chunks'] += len(chunks)
                
                # Generate embeddings
                embeddings = embedder.embed_documents(chunks, batch_size=32)
                
                # Upload raw data to GCS
                gcs_path = f"raw/sec/{ticker}/{filing['fiscal_year']}/{filing['accession_number']}_section_{section['section_code']}.json"
                gcs.upload_data(
                    data={
                        'filing_metadata': {
                            'ticker': ticker,
                            'filing_type': filing['filing_type'],
                            'accession_number': filing['accession_number']
                        },
                        'section': {'code': section['section_code'], 'name': section['section_name']},
                        'tables': tables,
                        'chunks': chunks
                    },
                    gcs_path=gcs_path
                )
                
                # Prepare comprehensive metadata payloads
                current_time = datetime.utcnow().isoformat() + 'Z'
                payloads = []
                
                # Generate table references
                table_refs = []
                if tables:
                    for t_idx, table in enumerate(tables):
                        table_refs.append(f"TABLE_{ticker}_{filing['accession_number']}_{section['section_code']}_{t_idx}")
                
                for i, chunk_text in enumerate(chunks):
                    chunk_tokens = len(chunker.encoding.encode(chunk_text))
                    
                    payloads.append({
                        # Core identifiers
                        'chunk_id': f"{ticker}_sec_{filing['accession_number']}_{section['section_code']}_{i}",
                        'ticker': ticker,
                        'company_name': company_name,  # Renamed from 'company'
                        'source': 'sec',  # Renamed from 'data_source'
                        
                        # Filing metadata
                        'filing_type': filing['filing_type'],
                        'filing_date': filing['filing_date'],
                        'fiscal_year': filing['fiscal_year'],
                        'fiscal_quarter': filing.get('fiscal_quarter'),
                        'accession_number': filing['accession_number'],
                        'filing_url': filing.get('filing_url', ''),
                        
                        # Section metadata
                        'section_code': section['section_code'],  # Renamed from 'section'
                        'section_title': section['section_name'],  # Renamed from 'section_name'
                        
                        # Chunk metadata
                        'chunk_index': i,
                        'total_chunks': len(chunks),
                        'chunk_text': chunk_text,
                        'chunk_size': len(chunk_text),  # Renamed from 'chunk_length'
                        'chunk_tokens': chunk_tokens,
                        
                        # Table metadata
                        'has_tables': len(tables) > 0,
                        'table_references': table_refs,
                        'num_tables': len(tables),
                        
                        # Storage
                        'gcs_path': gcs_path,
                        
                        # Timestamps
                        'processed_date': current_time,
                        'created_at': current_time,
                        'fetched_at': current_time,
                        'expires_at': None,
                        
                        # Bias mitigation
                        'boost_factor': 0.0,  # Default for large companies
                        'coverage_classification': 'medium'
                    })
                
                qdrant.upload_vectors(collection_name=collection_name, vectors=embeddings, payloads=payloads)
    
    except Exception as e:
        print(f"   ❌ SEC Error: {e}")
        import traceback
        traceback.print_exc()
    
    # === 2. Process Wikipedia with Comprehensive Metadata ===
    print("\n📖 Processing Wikipedia...")
    try:
        wiki_fetcher = WikipediaFetcher()
        page = wiki_fetcher.fetch_by_ticker(ticker)
        
        stats['wikipedia']['pages'] = 1
        
        # Chunk content
        chunks = chunker.chunk_text(page['text_content'])
        stats['wikipedia']['chunks'] = len(chunks)
        
        # Generate embeddings
        embeddings = embedder.embed_documents(chunks, batch_size=32)
        
        # Upload to GCS
        gcs_path = f"raw/wikipedia/{ticker}/{page['page_title'].replace(' ', '_')}.json"
        gcs.upload_data(
            data={
                'page_metadata': {'title': page['page_title']},
                'content': page['text_content'],
                'chunks': chunks
            },
            gcs_path=gcs_path
        )
        
        # Prepare comprehensive metadata
        current_time = datetime.utcnow().isoformat() + 'Z'
        payloads = []
        
        for i, chunk_text in enumerate(chunks):
            chunk_tokens = len(chunker.encoding.encode(chunk_text))
            
            payloads.append({
                # Core identifiers
                'chunk_id': f"{ticker}_wiki_{i}",
                'ticker': ticker,
                'company_name': company_name,
                'source': 'wikipedia',  # Renamed
                
                # Page metadata
                'page_title': page['page_title'],
                'page_url': page.get('page_url', ''),
                'revision_id': page.get('revision_id'),
                
                # Chunk metadata
                'chunk_index': i,
                'total_chunks': len(chunks),
                'chunk_text': chunk_text,
                'chunk_size': len(chunk_text),  # Renamed
                'chunk_tokens': chunk_tokens,
                
                # Table metadata
                'has_tables': False,
                'table_references': [],
                
                # Storage
                'gcs_path': gcs_path,
                
                # Timestamps
                'processed_date': current_time,
                'created_at': current_time,
                'fetched_at': current_time,
                'expires_at': None,
                
                # Bias mitigation
                'boost_factor': 0.12,
                'coverage_classification': 'medium'
            })
        
        qdrant.upload_vectors(collection_name=collection_name, vectors=embeddings, payloads=payloads)
        
        print(f"   ✓ Processed {len(chunks)} chunks")
    
    except Exception as e:
        print(f"   ❌ Wikipedia Error: {e}")
        import traceback
        traceback.print_exc()
    
    # === 3. Process News ===
    print("\n📰 Processing News...")
    try:
        news_fetcher = NewsFetcher(mode="auto")
        articles = news_fetcher.fetch_by_ticker(ticker, days_back=30, max_records=20)
        
        if articles:
            stats['news']['articles'] = len(articles)
            
            for article in articles:
                content = article.get('content', article.get('description', ''))
                if not content:
                    continue
                
                chunks = chunker.chunk_text(content)
                if not chunks:
                    continue
                
                stats['news']['chunks'] += len(chunks)
                embeddings = embedder.embed_documents(chunks, batch_size=32)
                
                # Calculate expiration
                pub_date_str = article.get('published_at') or article.get('publishedAt')
                if pub_date_str:
                    pub_date = datetime.fromisoformat(pub_date_str.replace('Z', '+00:00'))
                else:
                    pub_date = datetime.now()
                
                expires_at = pub_date + timedelta(days=180)
                
                # Upload to GCS
                gcs_path = f"raw/news/{ticker}/{pub_date.strftime('%Y%m%d')}_{article['url'].split('/')[-1][:50]}.json"
                gcs.upload_data(
                    data={'article': article, 'chunks': chunks},
                    gcs_path=gcs_path
                )
                
                # Prepare metadata
                current_time = datetime.utcnow().isoformat() + 'Z'
                article_id = article['url'].split('/')[-1][:50]
                source_name = article.get('source', {}).get('name', 'Unknown') if isinstance(article.get('source'), dict) else article.get('source', 'Unknown')
                
                payloads = []
                for i, chunk_text in enumerate(chunks):
                    chunk_tokens = len(chunker.encoding.encode(chunk_text))
                    
                    payloads.append({
                        # Core identifiers
                        'chunk_id': f"{ticker}_news_{article_id}_{i}",
                        'ticker': ticker,
                        'company_name': company_name,
                        'source': 'news',
                        
                        # Article metadata
                        'article_title': article['title'],
                        'article_url': article['url'],
                        'article_id': article_id,
                        'news_source': source_name,  # Renamed
                        'author': article.get('author', 'Unknown'),
                        'published_date': pub_date.isoformat() + 'Z',
                        
                        # Chunk metadata
                        'chunk_index': i,
                        'total_chunks': len(chunks),
                        'chunk_text': chunk_text,
                        'chunk_size': len(chunk_text),
                        'chunk_tokens': chunk_tokens,
                        
                        # Table metadata
                        'has_tables': False,
                        'table_references': [],
                        
                        # Storage
                        'gcs_path': gcs_path,
                        
                        # Timestamps
                        'processed_date': current_time,
                        'created_at': current_time,
                        'fetched_at': current_time,
                        'expires_at': expires_at.isoformat() + 'Z',
                        
                        # Bias mitigation
                        'boost_factor': 0.12,
                        'coverage_classification': 'medium'
                    })
                
                qdrant.upload_vectors(collection_name=collection_name, vectors=embeddings, payloads=payloads)
        
        print(f"   ✓ Processed {stats['news']['articles']} articles")
    
    except Exception as e:
        print(f"   ❌ News Error: {e}")
        import traceback
        traceback.print_exc()
    
    # Print summary
    print(f"\n{'='*80}")
    print("✅ PROCESSING COMPLETE")
    print(f"{'='*80}")
    print(f"SEC: {stats['sec']['filings']} filings, {stats['sec']['chunks']} chunks, {stats['sec']['tables']} tables")
    print(f"Wikipedia: {stats['wikipedia']['pages']} page, {stats['wikipedia']['chunks']} chunks")
    print(f"News: {stats['news']['articles']} articles, {stats['news']['chunks']} chunks")
    print(f"Total chunks: {stats['sec']['chunks'] + stats['wikipedia']['chunks'] + stats['news']['chunks']}")
    
    return stats
    """
    Process all data for a single company
    
    This is the main processing function that:
    1. Fetches SEC filings, Wikipedia, News
    2. Processes and chunks text
    3. Generates embeddings
    4. Uploads to cloud (GCS + Qdrant)
    """
    import sys
    from pathlib import Path
    import os
    from dotenv import load_dotenv
    
    # Setup paths for Docker container environment
    # In container: DAG is at /opt/airflow/dags/initial_load_dag.py
    #               Data_Pipeline is at /opt/airflow/Data_Pipeline/
    #               src module is at /opt/airflow/Data_Pipeline/src/
    
    # Add Data_Pipeline to Python path so we can import src.*
    data_pipeline_path = '/opt/airflow/Data_Pipeline'
    if data_pipeline_path not in sys.path:
        sys.path.insert(0, data_pipeline_path)
    
    # Load environment variables from /opt/airflow/.env
    load_dotenv(dotenv_path='/opt/airflow/.env')
    
    # Import connectors (actual classes that exist)
    from src.cloud.postgres_connector import PostgresConnector
    from src.cloud.qdrant_connector import QdrantConnector
    from src.cloud.gcs_connector import GCSConnector
    
    # Import processors - NOTE: These expect Manager classes but we have Connectors
    # We'll use connectors directly instead
    from src.data_ingestion.news_fetcher import NewsFetcher
    from src.data_processing.chunker import TextChunker
    from src.embedding import Embedder
    
    print(f"\n{'='*80}")
    print(f"PROCESSING: {company_name} ({ticker})")
    print(f"{'='*80}\n")
    
    # Initialize components
    print("🔧 Initializing components...")
    
    # Initialize PostgreSQL manager
    postgres_mgr = PostgresManager(
        host=os.getenv('POSTGRES_HOST'),
        port=int(os.getenv('POSTGRES_PORT', 5432)),
        database=os.getenv('POSTGRES_DB'),
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD')
    )
    
    # Initialize Qdrant manager
    qdrant_mgr = QdrantManager(
        url=os.getenv('QDRANT_URL'),
        api_key=os.getenv('QDRANT_API_KEY')
    )
    
    # Initialize GCS connector (for news processing)
    gcs = GCSConnector(
        bucket_name=os.getenv('GCP_BUCKET_NAME'),
        project_id=os.getenv('GCP_PROJECT_ID'),
        credentials_path=os.getenv('GCP_CREDENTIALS_PATH')
    )
    
    # Initialize embedder and LLM client
    embedder = FinancialEmbedder()
    llm_client = LLMClient()
    
    # Initialize processors with comprehensive metadata support
    sec_processor = SECProcessor(
        postgres_manager=postgres_mgr,
        qdrant_manager=qdrant_mgr,
        embedder=embedder,
        llm_client=llm_client
    )
    
    wiki_processor = WikipediaProcessor(
        postgres_manager=postgres_mgr,
        qdrant_manager=qdrant_mgr,
        embedder=embedder
    )
    
    # For news processing (still manual for now)
    news_embedder = Embedder()
    news_chunker = TextChunker(chunk_size=512, overlap=128)
    
    collection_name = os.getenv('QDRANT_COLLECTION', 'company_data')
    
    # Ensure collection exists
    try:
        qdrant_mgr.qdrant_client.create_collection(
            collection_name=collection_name,
            vector_size=1024,
            distance='Cosine',
            recreate=False
        )
    except:
        pass  # Collection already exists
    
    stats = {
        'sec': {'filings': 0, 'chunks': 0, 'tables': 0},
        'wikipedia': {'pages': 0, 'chunks': 0},
        'news': {'articles': 0, 'chunks': 0}
    }
    
    # === 1. Process SEC Filings ===
    print("\n📄 Processing SEC Filings...")
    try:
        # Use SECProcessor for comprehensive metadata and state tracking
        result = sec_processor.process_company_filings(
            ticker=ticker,
            filing_types=['10-K', '10-Q'],
            start_date='2023-01-01',
            skip_existing=True  # Skip filings already in PostgreSQL
        )
        
        stats['sec']['filings'] = result.get('processed', 0)
        stats['sec']['chunks'] = sum(
            d.get('total_chunks', 0) 
            for d in result.get('details', []) 
            if d.get('status') == 'success'
        )
        
        print(f"   ✓ Processed {stats['sec']['filings']} filings")
        print(f"   ✓ Generated {stats['sec']['chunks']} chunks")
        
        if result.get('failed', 0) > 0:
            print(f"   ⚠️  {result['failed']} filings failed")
    
    except Exception as e:
        print(f"   ❌ SEC Error: {e}")
    
    # === 2. Process Wikipedia ===
    print("\n📖 Processing Wikipedia...")
    try:
        # Use WikipediaProcessor for comprehensive metadata and state tracking
        result = wiki_processor.process_page(
            ticker=ticker,
            force_refresh=False  # Skip if not changed
        )
        
        if result['status'] == 'success':
            stats['wikipedia']['pages'] = 1
            stats['wikipedia']['chunks'] = result.get('total_chunks', 0)
            print(f"   ✓ Processed Wikipedia page")
            print(f"   ✓ Generated {stats['wikipedia']['chunks']} chunks")
        elif result['status'] == 'skipped':
            print(f"   ℹ️  Wikipedia page unchanged, skipped")
        else:
            print(f"   ❌ Failed: {result.get('error', 'Unknown error')}")
    
    except Exception as e:
        print(f"   ❌ Wikipedia Error: {e}")
    
    # === 3. Process News ===
    print("\n📰 Processing News...")
    try:
        news_fetcher = NewsFetcher(mode='auto')
        articles = news_fetcher.fetch_by_ticker(ticker=ticker, days_back=30, max_records=20)
        
        stats['news']['articles'] = len(articles)
        
        for article in articles:
            chunks = chunker.chunk_text(article['content'])
            if not chunks:
                continue
            
            stats['news']['chunks'] += len(chunks)
            
            # Generate embeddings
            embeddings = embedder.embed_documents(chunks, batch_size=32)
            
            # Upload to GCS
            pub_date = datetime.now()  # Simplified
            gcs_path = f"raw/news/{ticker}/{pub_date.strftime('%Y%m%d')}_{article['url'].split('/')[-1][:50]}.json"
            gcs.upload_data(
                data={'article_metadata': {'title': article['title']}, 'content': article['content'], 'chunks': chunks},
                gcs_path=gcs_path
            )
            
            # Upload to Qdrant
            current_time = datetime.utcnow().isoformat() + 'Z'
            payloads = []
            for i, chunk_text in enumerate(chunks):
                chunk_tokens = len(chunker.encoding.encode(chunk_text))
                payloads.append({
                    'chunk_id': f"{ticker}_news_{i}",
                    'ticker': ticker,
                    'company': company_name,
                    'data_source': 'news',
                    'article_title': article['title'],
                    'chunk_text': chunk_text,
                    'chunk_tokens': chunk_tokens,
                    'created_at': current_time
                })
            
            qdrant.upload_vectors(collection_name=collection_name, vectors=embeddings, payloads=payloads)
    
    except Exception as e:
        print(f"   ❌ News Error: {e}")
    
    # Summary
    print(f"\n✅ COMPLETED: {ticker}")
    print(f"   SEC: {stats['sec']['filings']} filings, {stats['sec']['chunks']} chunks")
    print(f"   Wikipedia: {stats['wikipedia']['pages']} page, {stats['wikipedia']['chunks']} chunks")
    print(f"   News: {stats['news']['articles']} articles, {stats['news']['chunks']} chunks")
    
    return stats


# === DAG Tasks ===

start = DummyOperator(task_id='start', dag=dag)

load_companies_task = PythonOperator(
    task_id='load_companies',
    python_callable=load_companies,
    provide_context=True,
    dag=dag,
)

# For now, create a simple task that processes AAPL as an example
# In production, you'd use dynamic task mapping for all companies

process_aapl = PythonOperator(
    task_id='process_AAPL',
    python_callable=process_company_data,
    op_kwargs={
        'ticker': 'AAPL',
        'company_name': 'Apple Inc.',
        'cik': '0000320193'
    },
    provide_context=True,
    execution_timeout=timedelta(hours=2),
    dag=dag,
)

end = DummyOperator(task_id='end', dag=dag)

# Task dependencies
start >> load_companies_task >> process_aapl >> end
