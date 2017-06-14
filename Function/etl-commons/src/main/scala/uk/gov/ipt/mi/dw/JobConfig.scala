package uk.gov.ipt.mi.dw

import java.io.{File, FileInputStream}
import java.util.Properties

case class JobConfig(jobConfigFile: File = new File("."))

case class DbConfig(user: String, passwd: String, jdbcUrl: String)

case class MIPersonVaultConfig(hubPersonInputPath: String, satPersonVisInputPath: String, satPersonChgLogInputPath: String, lnkPersonIdInputPath: String)

case class MIIdentityHubVaultConfig(hubIdentityInputPath: String)

case class MIIdentitySatVaultConfig(satIdentityInfoInputPath: String, satIdentityBiomInfoInputPath: String, satIdentityRefInputPath: String, satIdentityCondInputPath: String, satIdentityBiogInputPath: String, satIdentityDescrInputPath: String, satIdentityMedInputPath: String, lnkIdentityPerInputPath: String)

case class MISrvcDlvryHubVaultConfig(hubSrvcDlvryCoreInputPath: String)

case class MISrvcDlvrySatVaultConfig(satSrvcDlvryInfoInputPath: String, satSrvcDlvryAttributeInputPath: String,
                                     satSrvcDlvryProcessInputPath: String, satSrvcDlvryProcessInstanceInputPath: String,
                                     lnkSrvcDlvryParentInputPath: String)


case class MIVaultConfig(personVaultConfig: MIPersonVaultConfig, dbConfig: DbConfig,
                         identityHubVaultConfig: MIIdentityHubVaultConfig, identitySatVaultConfig: MIIdentitySatVaultConfig,
                         srvcDlvryHubVaultConfig: MISrvcDlvryHubVaultConfig, srvcDlvrySatVaultConfig : MISrvcDlvrySatVaultConfig )

object JobConfig {
  def loadConfig(jobConfig: JobConfig) : MIVaultConfig = {
    val prop = new Properties()
    prop.load(new FileInputStream(jobConfig.jobConfigFile))

    val personVaultConfig = MIPersonVaultConfig(
      hubPersonInputPath = prop.getProperty("mi.vault.person.hub.input"),
      satPersonVisInputPath = prop.getProperty("mi.vault.person.sat.visibility.input"),
      satPersonChgLogInputPath = prop.getProperty("mi.vault.person.sat.changelog.input"),
      lnkPersonIdInputPath = prop.getProperty("mi.vault.lnk.person.identity.input")
     )

    val dbConfig = DbConfig(
    user = prop.getProperty("mi.vault.db.user"),
    passwd = prop.getProperty("mi.vault.db.pwd"),
    jdbcUrl = "jdbc:postgresql://svram01.np-ii-dat1-ciz1.ipt.ho.local:5432/mi_warehouse"
    )

    val identityHubVaultConfig = MIIdentityHubVaultConfig(
      hubIdentityInputPath = prop.getProperty("mi.vault.identity.hub.input")
    )

    val identitySatVaultConfig = MIIdentitySatVaultConfig(
      satIdentityInfoInputPath = prop.getProperty("mi.vault.identity.sat.info.input"),
      satIdentityBiomInfoInputPath = prop.getProperty("mi.vault.identity.sat.biometricinfo.input"),
      satIdentityRefInputPath = prop.getProperty("mi.vault.identity.sat.reference.input"),
      satIdentityCondInputPath = prop.getProperty("mi.vault.identity.sat.condition.input"),
      satIdentityBiogInputPath = prop.getProperty("mi.vault.identity.sat.biographics.input"),
      satIdentityDescrInputPath = prop.getProperty("mi.vault.identity.sat.descriptors.input"),
      satIdentityMedInputPath = prop.getProperty("mi.vault.identity.sat.medias.input"),
      lnkIdentityPerInputPath = prop.getProperty("mi.vault.lnk.identity.person.input")
    )

    val srvcDlvryHubVaultConfig = MISrvcDlvryHubVaultConfig(
      hubSrvcDlvryCoreInputPath = prop.getProperty("mi.vault.servicedelivery.hub.core.input")
    )

    val srvcDlvrySatVaultConfig = MISrvcDlvrySatVaultConfig(
      satSrvcDlvryInfoInputPath = prop.getProperty("mi.vault.servicedelivery.satellite.info.core.input"),
      satSrvcDlvryAttributeInputPath = prop.getProperty("mi.vault.servicedelivery.satellite.attribute.core.input"),
      satSrvcDlvryProcessInputPath = prop.getProperty("mi.vault.servicedelivery.satellite.process.core.input"),
      satSrvcDlvryProcessInstanceInputPath = prop.getProperty("mi.vault.servicedelivery.satellite.processinstance.core.input"),
      lnkSrvcDlvryParentInputPath = prop.getProperty("mi.vault.servicedelivery.link.parent.core.input")
    )

    MIVaultConfig(personVaultConfig, dbConfig,
                  identityHubVaultConfig, identitySatVaultConfig,
                  srvcDlvryHubVaultConfig,srvcDlvrySatVaultConfig)
  }
}
