import smv
import pyspark.sql.functions as F

class CONCEPT(smv.iomod.SmvCsvInputFile):
    def connectionName(self):
        return "myinput"

    def fileName(self):
        return "CONCEPT.csv"

    def csvReaderMode(self):
        return "DROPMALFORMED"

class RELATIONSHIP(smv.iomod.SmvCsvInputFile):
    def connectionName(self):
        return "myinput"

    def fileName(self):
        return "RELATIONSHIP.csv"

    def csvReaderMode(self):
        return "DROPMALFORMED"

class VOCABULARY(smv.iomod.SmvCsvInputFile):
    def connectionName(self):
        return "myinput"

    def fileName(self):
        return "VOCABULARY.csv"

class DOMAIN(smv.iomod.SmvCsvInputFile):
    def connectionName(self):
        return "myinput"

    def fileName(self):
        return "DOMAIN.csv"

    def csvReaderMode(self):
        return "DROPMALFORMED"

class CONCEPT_ANCESTOR(smv.iomod.SmvCsvInputFile):
    def connectionName(self):
        return "myinput" 

    def fileName(self):
        return "CONCEPT_ANCESTOR.csv"

    def csvReaderMode(self):
        return "DROPMALFORMED"

class CONCEPT_CLASS(smv.iomod.SmvCsvInputFile):
    def connectionName(self):
        return "myinput"

    def fileName(self):
        return "CONCEPT_CLASS.csv" 

    def csvReaderMode(self):
        return "DROPMALFORMED"

class CONCEPT_RELATIONSHIP(smv.iomod.SmvCsvInputFile):
    def connectionName(self):
        return "myinput"

    def fileName(self):
        return "CONCEPT_RELATIONSHIP.csv"

    def csvReaderMode(self):
        return "DROPMALFORMED"

class CONCEPT_SYNONYM(smv.iomod.SmvCsvInputFile):
    def connectionName(self):
        return "myinput"

    def fileName(self):
        return "CONCEPT_SYNONYM.csv"

    def csvReaderMode(self):
        return "DROPMALFORMED"

class DRUG_STRENGTH(smv.iomod.SmvCsvInputFile):
    def connectionName(self):
        return "myinput"

    def fileName(self):
        return "DRUG_STRENGTH.csv"

    def csvReaderMode(self):
        return "DROPMALFORMED"

#class EmploymentByState(smv.SmvModule):
#    """Python ETL Example: employ by state"""

#    def requiresDS(self):
#        return [Employment]

#    def run(self, i):
#        df = i[Employment]
#        return df.groupBy(F.col("ST")).agg(F.sum(F.col("EMP")).alias("EMP"))

#class EmploymentByStateOut(smv.iomod.SmvCsvOutputFile):
#    def requiresDS(self):
#        return [EmploymentByState]

#    def connectionName(self):
#        return "myoutput"

#    def fileName(self):
#        return "employment_by_state.csv"
