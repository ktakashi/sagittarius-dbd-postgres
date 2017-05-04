(formula
 (description "PostgreSQL DBD for Sagittarius")
 (version "HEAD")
 (homepage :url "https://github.com/ktakashi/sagittarius-dbd-postgres")
 (dependencies (dependency :name "postgresql" :version "HEAD"))
 (author :name "Takashi Kato" :email "ktakashi@ymail.com")
 (source 
  ;;:type tar :compression gzip
  :type zip
  :url "https://github.com/ktakashi/sagittarius-dbd-postgres/archive/master.zip")

 (install (directories ("lib"))))
