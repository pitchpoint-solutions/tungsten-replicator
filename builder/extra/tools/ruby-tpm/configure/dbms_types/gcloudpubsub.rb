DBMS_GCLOUDPUBSUB = "gcloudpubsub"

# GCloudPubSub-specific parameters.
class GCloudPubSubPlatform < ConfigureDatabasePlatform
  def get_uri_scheme
    DBMS_GCLOUDPUBSUB
  end

  def get_default_backup_method
    "none"
  end

  def get_valid_backup_methods
    "none|script"
  end

  def get_thl_uri
    nil
  end

  def get_default_port
    nil
  end

  def get_default_start_script
    nil
  end

  def getBasicJdbcUrl()
    nil
  end

  def getJdbcUrl()
    nil
  end

  def getJdbcDriver()
    nil
  end

  def getVendor()
    "GCloud"
  end

  def get_extractor_template
    raise "Unable to use GCloudPubSubPlatform as an extractor"
  end

  def get_applier_filters()
    []
  end

  def get_default_master_log_directory
    nil
  end

  def get_default_master_log_pattern
    nil
  end
end

#
# Prompts
#

class GCloudPubSubConfigurePrompt < ConfigurePrompt
  def get_default_value
    begin
      if Configurator.instance.display_help? && !Configurator.instance.display_preview?
        raise ""
      end

      get_gcloudpubsub_default_value()
    rescue => e
      super()
    end
  end

  def get_gcloudpubsub_default_value
    raise "Undefined function"
  end

  # Execute mysql command and return result to client. 
  def gcloudpubsub(command, hostname = nil)
    user = @config.getProperty(REPL_DBLOGIN)
    password = @config.getProperty(REPL_DBPASSWORD)
    port = @config.getProperty(REPL_DBPORT)
    if hostname == nil
      hosts = @config.getProperty(HOSTS).split(",")
      hostname = hosts[0]
    end

    raise "Update this to build the proper command"
  end

  def enabled?
    super() && (get_datasource().is_a?(GCloudPubSubPlatform))
  end

  def enabled_for_config?
    super() && (get_datasource().is_a?(GCloudPubSubPlatform))
  end
end

#
# Validation
#
class GCloudPubSubValidationCheck < ConfigureValidationCheck
  def get_variable(name)
    mysql("show #{name}").chomp.strip;
  end

  def enabled?
    super() && @config.getProperty(REPL_DBTYPE) == DBMS_GCLOUDPUBSUB
  end
end