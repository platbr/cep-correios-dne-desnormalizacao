require 'rom'
require 'byebug'
require 'csv'

FILE_ENCODING = 'ISO-8859-1'
DATABASE_FILE = 'dne_importado.db'
IMPORT_DATA = !File.exists?(DATABASE_FILE)
rom = ROM.container(:sql, "sqlite://#{DATABASE_FILE}") do |conf|
  # CAMPO	DESCRIÇÃO DO CAMPO	TIPO
  # LOC_NU	chave da localidade	NUMBER(8)
  # SEPARADOR	@	
  # UFE_SG	sigla da UF	CHAR(2)
  # SEPARADOR	@	
  # LOC_NO	nome da localidade	VARCHAR(72)
  # SEPARADOR	@	
  # CEP	CEP da localidade (para  localidade  não codificada, ou seja loc_in_sit = 0) (opcional)	CHAR(8)
  # SEPARADOR	@	
  # LOC_IN_SIT	situação da localidade:
  # 0 = Localidade  não codificada em nível de Logradouro,
  # 1 = Localidade codificada em nível de Logradouro e
  # 2 = Distrito ou Povoado inserido na codificação em nível de Logradouro.
  # 3 = Localidade em fase de codificação  em nível de Logradouro.	CHAR(1)
  # SEPARADOR	@	
  # LOC_IN_TIPO_LOC	tipo de localidade:
  # D – Distrito,
  # M – Município,
  # P – Povoado.	
  # CHAR(1)
  # SEPARADOR	@	
  # LOC_NU_SUB	chave da localidade de subordinação (opcional)	NUMBER(8)
  # SEPARADOR	@	
  # LOC_NO_ABREV	abreviatura do nome da localidade (opcional)	VARCHAR(36)
  # SEPARADOR	@	
  # MUN_NU	Código do município IBGE (opcional)	CHAR(7)
  conf.default.create_table(:localidades) do
    primary_key :LOC_NU
    column :UFE_SG, String, null: false
    column :LOC_NO, String, null: false
    column :CEP, String, null: false
    column :LOC_IN_SIT, String, null: false
    column :LOC_IN_TIPO_LOC, String, null: false
    column :LOC_NU_SUB, Integer, null: true
    column :LOC_NO_ABREV, String, null: true
    column :MUN_NU, Integer, null: true
  end if IMPORT_DATA

  class Localidades < ROM::Relation[:sql]
    schema(infer: true) do
      associations do
        has_many :bairros
        has_many :logradouros
        has_one :faixa_cep_localidades
      end
    end
  end

  conf.register_relation(Localidades)

  # CAMPO	DESCRIÇÃO DO CAMPO	TIPO
  # BAI_NU	chave do bairro	NUMBER(8)
  # SEPARADOR	@	
  # UFE_SG	sigla da UF	CHAR(2)
  # SEPARADOR	@	
  # LOC_NU	chave da localidade	NUMBER(8)
  # SEPARADOR	@	
  # BAI_NO	nome do bairro	VARCHAR2(72)
  # SEPARADOR	@	
  # BAI_NO_ABREV	abreviatura do nome do bairro (opcional)	VARCHAR2(36)

  conf.default.create_table(:bairros) do
    primary_key :BAI_NU
    column :UFE_SG, String, null: false
    column :LOC_NU, Integer, null: false
    column :BAI_NO, String, null: false
    column :BAI_NO_ABREV, String, null: true
  end if IMPORT_DATA

  class Bairros < ROM::Relation[:sql]
    schema(infer: true) do
      associations do
        belongs_to :localidade, foreign_key: :LOC_NU
      end
    end
  end

  conf.register_relation(Bairros)

  # CAMPO	DESCRIÇÃO DO CAMPO	TIPO
  # LOG_NU	chave do logradouro	NUMBER(8)
  # SEPARADOR	@	
  # UFE_SG	sigla da UF	CHAR(2)
  # SEPARADOR	@	
  # LOC_NU	chave da localidade	NUMBER(8)
  # SEPARADOR	@	
  # BAI_NU_INI	chave do bairro inicial do logradouro 	NUMBER(8)
  # SEPARADOR	@	
  # BAI_NU_FIM	chave do bairro final do logradouro (opcional)	NUMBER(8)
  # SEPARADOR	@	
  # LOG_NO	nome do logradouro	VARCHAR2(100)
  # SEPARADOR	@	
  # LOG_COMPLEMENTO	complemento do logradouro (opcional)	VARCHAR2(100)
  # SEPARADOR	@	
  # CEP	CEP do logradouro	CHAR(8)
  # SEPARADOR	@	
  # TLO_TX	tipo de logradouro	VARCHAR2(36)
  # SEPARADOR	@	
  # LOG_STA_TLO	indicador de utilização do tipo de logradouro (S ou N) (opcional)	CHAR(1)
  # SEPARADOR	@	
  # LOG_NO_ABREV	abreviatura do nome do logradouro (opcional)	VARCHAR2(36)
  
  conf.default.create_table(:logradouros) do
    primary_key :LOG_NU
    column :UFE_SG, String, null: false
    column :LOC_NU, Integer, null: false
    column :BAI_NU_INI, Integer, null: false
    column :BAI_NU_FIM, Integer, null: true
    column :LOG_NO, String, null: false
    column :LOG_COMPLEMENTO, String, null: false
    column :CEP, String, null: false
    column :TLO_TX, String, null: false
    column :LOG_STA_TLO, String, null: true
    column :LOG_NO_ABREV, String, null: true
  end if IMPORT_DATA

  class Logradouros < ROM::Relation[:sql]
    schema(infer: true) do
      associations do
        belongs_to :localidade, foreign_key: :LOC_NU
        belongs_to :bairro, foreign_key: :BAI_NU_INI
      end
    end
  end

  conf.register_relation(Logradouros)

  # CAMPO	DESCRIÇÃO DO CAMPO	TIPO
  # LOC_NU	chave da localidade	NUMBER(8)
  # SEPARADOR	@	
  # LOC_CEP_INI	CEP inicial da localidade	CHAR(8)
  # SEPARADOR	@	
  # LOC_CEP_FIM	CEP final da localidade	CHAR(8)
  # SEPARADOR	@	
  # LOC_TIPO_FAIXA	tipo de Faixa de CEP:
  # T –Total do Município 
  # C – Exclusiva da  Sede Urbana	CHAR(1)

  conf.default.create_table(:faixa_cep_localidades) do
    column :LOC_NU, Integer, null: false
    column :LOC_CEP_INI, String, null: false
    column :LOC_CEP_FIM, String, null: false
    column :LOC_TIPO_FAIXA, String, null: false
  end if IMPORT_DATA

  class FaixaCepLocalidades < ROM::Relation[:sql]
    schema(infer: true) do
      associations do
        belongs_to :localidade, foreign_key: :LOC_NU
      end
    end
  end

  conf.register_relation(FaixaCepLocalidades)

end

LocalidadesRelation = rom.relations[:localidades]
BairrosRelation = rom.relations[:bairros]
LogradourosRelation = rom.relations[:logradouros]
FaixaCepLocalidadesRelation = rom.relations[:faixa_cep_localidades]

if IMPORT_DATA
  File.readlines("dados/Delimitado/LOG_LOCALIDADE.TXT", encoding: FILE_ENCODING).each do |line|
    line = line.strip.split('@')
    LocalidadesRelation.changeset(:create, LOC_NU: line[0], UFE_SG: line[1], LOC_NO: line[2], CEP: line[3], LOC_IN_SIT: line[4], LOC_IN_TIPO_LOC: line[5], LOC_NU_SUB: line[6], LOC_NO_ABREV: line[7], MUN_NU: line[8]).commit
  end

  File.readlines("dados/Delimitado/LOG_BAIRRO.TXT", encoding: FILE_ENCODING).each do |line|
    line = line.strip.split('@')
    BairrosRelation.changeset(:create, BAI_NU: line[0], UFE_SG: line[1], LOC_NU: line[2], BAI_NO: line[3], BAI_NO_ABREV: line[4]).commit
  end

  Dir["dados/Delimitado/LOG_LOGRADOURO_*.TXT"].each do |file_logradouro|
    File.readlines(file_logradouro, encoding: FILE_ENCODING).each do |line|
      line = line.strip.split('@')
      LogradourosRelation.changeset(:create, LOG_NU: line[0], UFE_SG: line[1], LOC_NU: line[2], BAI_NU_INI: line[3], BAI_NU_FIM: line[4], LOG_NO: line[5], LOG_COMPLEMENTO: line[6], CEP: line[7], TLO_TX: line[8], LOG_STA_TLO: line[9], LOG_NO_ABREV: line[10]).commit
    end
  end

  File.readlines("dados/Delimitado/LOG_FAIXA_LOCALIDADE.TXT", encoding: FILE_ENCODING).each do |line|
    line = line.strip.split('@')
    FaixaCepLocalidadesRelation.changeset(:create, LOC_NU: line[0], LOC_CEP_INI: line[1], LOC_CEP_FIM: line[2], LOC_TIPO_FAIXA: line[3]).commit
  end  
end


CSV.open("ceps.csv", "w", col_sep: ';') do |csv|
  csv << ["cep", "logradouro", "bairro", "localidade", "localidade_ibge_cod", "uf"]

  LogradourosRelation.combine(:localidade, :bairro).each do |logradouro|
    csv << [
      logradouro[:CEP],
      "#{logradouro[:TLO_TX]} #{logradouro[:LOG_NO]}",
      logradouro[:bairro][:BAI_NO],
      logradouro[:localidade][:UFE_SG],
      logradouro[:localidade][:LOC_NO],
      logradouro[:localidade][:MUN_NU]
    ]
  end
end

CSV.open("faixas.csv", "w", col_sep: ';') do |csv|
  csv << ["cep_inicio","cep_fim", "localidade", "localidade_ibge_cod", "uf"]
  FaixaCepLocalidadesRelation.combine(:localidade).each do |faixa|
    csv << [
      faixa[:LOC_CEP_INI],
      faixa[:LOC_CEP_FIM],
      faixa[:localidade][:LOC_NO],
      faixa[:localidade][:MUN_NU],
      faixa[:localidade][:UFE_SG]
    ]
  end
end