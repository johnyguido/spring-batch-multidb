package com.example.transactionsmultiplesdbs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.repeat.CompletionPolicy;
import org.springframework.batch.repeat.policy.CompletionPolicySupport;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.util.List;
import java.util.stream.Collectors;

@Configuration
public class BatchConfig {

  private static final Logger logger = LoggerFactory.getLogger(BatchConfig.class);

  @Autowired
  @Qualifier("springDS")
  private DataSource dataSourceMeta;

  @Autowired
  @Qualifier("appDS")
  private DataSource dataSourceApp;

  @Autowired
  @Qualifier("transactionManagerApp")
  private PlatformTransactionManager transactionManager;

  @Autowired
  @Qualifier("transactionManagerMeta")
  private PlatformTransactionManager transactionManagerMeta;

  @Bean
  public Job job(JobRepository jobRepository, Step step) {
    logger.info("Creating job {}", step.getName());
    return new JobBuilder("job", jobRepository)
            .start(step)
            .incrementer(new RunIdIncrementer())
            .listener(metadataCleanupListener())
            .build();
  }

  @Bean
  public Step step(ItemReader<Pessoa> reader, ItemWriter<Pessoa> writer, JobRepository jobRepository) {
    logger.info("Creating step {}",jobRepository.getJobNames());
    return new StepBuilder("step", jobRepository)
        .<Pessoa, Pessoa>chunk(200, this.transactionManager)
        .reader(reader)
        .writer(writer)
        .build();
  }

  @Bean
  public ItemReader<Pessoa> reader() {
    logger.info("Creating reader");
    return new FlatFileItemReaderBuilder<Pessoa>()
        .name("reader")
        .resource(new FileSystemResource("files/pessoas.csv"))
        .comments("--")
        .delimited()
        .names("nome", "email", "dataNascimento", "idade", "id")
        .targetType(Pessoa.class)
        .build();
  }

  @Bean
  public ItemWriter<Pessoa> writer(@Qualifier("appDS") DataSource dataSource) {
    logger.info("Creating writer");
    return new JdbcBatchItemWriterBuilder<Pessoa>()
        .dataSource(dataSource)
        .sql(
            "INSERT INTO pessoa (id, nome, email, data_nascimento, idade) VALUES (:id, :nome, :email, :dataNascimento, :idade)")
        .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
        .build();
  }

  @Bean
  public JdbcTemplate jdbcTemplate() {
    logger.info("Creating jdbcTemplate");
    return new JdbcTemplate(this.dataSourceMeta);
  }

  @Bean
  public JdbcTemplate jdbcTemplateSpring() {
    logger.info("Creating jdbcTemplate");
    return new JdbcTemplate(this.dataSourceApp);
  }


  // Cria uma instância do MetadataCleanupListener
  @Bean
  public MetadataCleanupListener metadataCleanupListener() {
    return new MetadataCleanupListener(jdbcTemplate(), jdbcTemplateSpring(), this.dataSourceApp, this.dataSourceMeta);
  }


  public class MetadataCleanupListener implements JobExecutionListener {

    private final JdbcTemplate jdbcTemplate;
    private final JdbcTemplate jdbcTemplateSpring;

    @Qualifier("appDS")
    private final DataSource appDS;

    // Injeta o JdbcTemplate
    public MetadataCleanupListener(JdbcTemplate jdbcTemplate,JdbcTemplate jdbcTemplateSpring, @Qualifier("appDS")DataSource appDS, @Qualifier("springDS")DataSource springDS) {
      this.jdbcTemplate = new JdbcTemplate(appDS);
      this.jdbcTemplateSpring = new JdbcTemplate(springDS);
      this.appDS = appDS;
    }

    // Executa antes da execução do job
    @Override
    public void beforeJob(JobExecution jobExecution) {

      String sql = "SHOW TABLES";

      // Executando a consulta e recuperando os resultados
      List<String> tables = jdbcTemplate.queryForList(sql, String.class);

      // Exibindo as tabelas no log
      System.out.println("Tabelas no banco de dados MYSQL:");
      for (String table : tables) {
        System.out.println(table);
      }

      System.out.println("--------------------------------------------------------------------------------------------");

      String sqlSpring = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='PUBLIC'";
      List<String> tablesSpring = jdbcTemplateSpring.queryForList(sqlSpring, String.class);

      System.out.println("Tabelas no banco de dados H2:");
      for (String table : tablesSpring) {
        System.out.println(table);
      }

      logger.info("Executing BEFORE JOB");
//      try {
//        System.out.println("PERIOD DE PAUSA PARA EXCLUSAO DE METADADOS " + 10000);
//        Thread.sleep(15000); // Pausa por 10 segundos (ajuste conforme necessário)
//      } catch (InterruptedException e) {
//        Thread.currentThread().interrupt();
//      }
      // Executa o script SQL para limpar as tabelas de metadados do Spring Batch
//      jdbcTemplate.execute("DELETE FROM BATCH_STEP_EXECUTION_CONTEXT");
//      jdbcTemplate.execute("DELETE FROM BATCH_STEP_EXECUTION");
//      jdbcTemplate.execute("DELETE FROM BATCH_JOB_EXECUTION_CONTEXT");
//      jdbcTemplate.execute("DELETE FROM BATCH_JOB_EXECUTION_PARAMS");
//      jdbcTemplate.execute("DELETE FROM BATCH_JOB_EXECUTION");
//      jdbcTemplate.execute("DELETE FROM BATCH_JOB_INSTANCE");
//      jdbcTemplate.execute("DELETE FROM BATCH_STEP_EXECUTION_SEQ");
//      jdbcTemplate.execute("DELETE FROM BATCH_JOB_EXECUTION_SEQ");
//      jdbcTemplate.execute("DELETE FROM BATCH_JOB_SEQ");
      int countPessoa = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM pessoa", Integer.class);
      logger.info("Found {} pessoas", countPessoa);
      jdbcTemplate.execute("DELETE FROM pessoa");
    }

    // Executa após a execução do job (não utilizado neste exemplo)
    @Override
    public void afterJob(JobExecution jobExecution) {
      int countPessoa = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM pessoa", Integer.class);
      logger.info("Found {} pessoas before Job", countPessoa);
      logger.info("Executing AFTER JOB");
      jdbcTemplate.execute("DELETE FROM pessoa");
    }
  }

}
