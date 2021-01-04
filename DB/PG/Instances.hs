{-# OPTIONS_GHC -Wno-orphans #-}
{-# LANGUAGE QuasiQuotes, ExtendedDefaultRules #-}
module DB.PG.Instances where

import Control.Newtype as Newtype
import Database.PostgreSQL.Simple
import Database.PostgreSQL.Simple.FromField
import Database.PostgreSQL.Simple.FromRow
import Database.PostgreSQL.Simple.ToField
import Data.Proxy
import Data.Scientific
import Data.Text (Text)
import qualified Data.Text as Text
import Text.InterpolatedString.Perl6 (qc)

import DB.PG
import Data.MOEX

instance FromRow Symbol where
  fromRow = Symbol <$> field

instance ToField Symbol where
  toField x = toField (Newtype.unpack x)

instance FromField Symbol where
  fromField f x = Symbol <$> fromField f x

instance FromField BondShortName where
  fromField f x = BondShortName <$> fromField f x

instance FromField BondFullName where
  fromField f x = BondFullName <$> fromField f x

instance ToField Market where
  toField (Market x) = toField x

instance ToField Price where
  toField (Price x) = toField (realToFrac x :: Scientific)

instance ToField Volume where
  toField (Volume x) = toField (realToFrac x :: Scientific)

instance HasColumn (Proxy Symbol) where
  column = const "symbol"

instance HasColumn (Proxy BondShortName) where
  column = const "value"

instance HasColumn (Proxy BondFullName) where
  column = const "value"

instance (HasColumn (Proxy a), HasColumn (Proxy b)) => HasColumns (Proxy (a, b)) where
  columns = const [ column (Proxy @a), column (Proxy @b) ]

newtype AllFrom = AllFrom Text

instance HasTable AllFrom PostgreSQLEngine where
  tablename _ (AllFrom x) = x

instance (HasTable q PostgreSQLEngine, HasColumns (Proxy a), FromRow a)
  => SelectStatement [a] q IO PostgreSQLEngine
      where
        select eng q = do
          conn <- getConnection eng
          query_ conn [qc|select {cols} from {table}|]
          where table = tablename eng q
                cols  = Text.intercalate "," (columns (Proxy :: Proxy a))

