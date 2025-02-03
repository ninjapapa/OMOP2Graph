import smv
import pyspark.sql.functions as F
import omop.input as input

class CONCEPT_WITH_CHILDREN(smv.SmvModule):
    """Create a dataframe with the concept which has children"""
    def requiresDS(self):
        return [
            input.CONCEPT_ANCESTOR,
            input.CONCEPT
        ]

    def run(self, i):
        df = i[input.CONCEPT_ANCESTOR]
        cc = i[input.CONCEPT]

        max_level_cnt = df.groupBy(F.col("ancestor_concept_id")).agg(F.max(F.col("max_levels_of_separation")).alias("max_levels"))

        # there are ancestor concepts where do not covered in CONCEPT table (I guess it is because of data download filter does not cover
        # CONCEPT_ANCESTOR table)
        return cc.join(max_level_cnt, F.col("concept_id") == F.col("ancestor_concept_id"), how="left")\
            .withColumn("max_levels", F.when(F.col("max_levels").isNull(), 0).otherwise(F.col("max_levels")))

class STUDY_DOMAIN_DEEPTH(smv.SmvModule):
    """Create a dataframe with the study domain depth"""
    def requiresDS(self):
        return [
            CONCEPT_WITH_CHILDREN
        ]
    
    def run(self, i):
        df = i[CONCEPT_WITH_CHILDREN]

        return df.groupBy("domain_id", "max_levels").count()
    
class STUDY_VOCABULARY_DEEPTH(smv.SmvModule):
    """Create a dataframe with the study vocabulary depth"""
    def requiresDS(self):
        return [
            CONCEPT_WITH_CHILDREN
        ]
    
    def run(self, i):
        df = i[CONCEPT_WITH_CHILDREN]

        return df.groupBy("vocabulary_id", "max_levels").count()


class STUDY_DOMAIN_VOCABULARY_DEEPTH(smv.SmvModule):
    """Create a dataframe with the study domain and vocabulary depth"""
    def requiresDS(self):
        return [
            CONCEPT_WITH_CHILDREN
        ]
    
    def run(self, i):
        df = i[CONCEPT_WITH_CHILDREN]

        return df.groupBy("domain_id", "vocabulary_id", "max_levels").count()


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
