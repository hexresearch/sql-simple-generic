{-# OPTIONS_GHC -Wno-orphans #-}
{-# LANGUAGE QuasiQuotes, ExtendedDefaultRules #-}
module DB.PG.Instances where

import Control.Newtype as Newtype
import Database.PostgreSQL.Simple
import Database.PostgreSQL.Simple.FromField
import Database.PostgreSQL.Simple.FromRow
import Database.PostgreSQL.Simple.ToField
import Data.Data
import Data.Proxy
import Data.Scientific
import Data.String
import Data.Text.Conversions
import Data.Text (Text)
import Data.Time.Calendar
import GHC.Generics
import GHC.TypeLits (KnownSymbol(..), symbolVal)
import qualified Data.Text as Text
import qualified GHC.TypeLits as T
import Text.InterpolatedString.Perl6 (qc)

import DB.PG
import Data.MOEX

instance FromRow Symbol where
  fromRow = Symbol <$> field

instance ToField Symbol where
  toField x = toField (Newtype.unpack x)

instance FromField Symbol where
  fromField f x = Symbol <$> fromField f x

instance FromField Volume where
  fromField f x = (Volume <$> realToFrac) <$> fromField f x

instance FromField BondCouponFreq where
  fromField f x = BondCouponFreq <$> fromField f x

instance FromField BondIssueDate where
  fromField f x = BondIssueDate <$> fromField f x

instance FromField BondMatDate where
  fromField f x = BondMatDate <$> fromField f x

instance FromField BondShortName where
  fromField f x = BondShortName <$> fromField f x

instance FromField BondFullName where
  fromField f x = BondFullName <$> fromField f x

instance FromField TradeAggDate where
  fromField f x = TradeAggDate <$> fromField f x

instance FromField TradeAggVol where
  fromField f x = TradeAggVol <$> fromField f x

instance ToField Market where
  toField (Market x) = toField x

instance ToField Price where
  toField (Price x) = toField (realToFrac x :: Scientific)

instance ToField Volume where
  toField (Volume x) = toField (realToFrac x :: Scientific)

data From (table :: T.Symbol) cols = All
data Rows cols

type family SelectRowType a :: *
type instance SelectRowType (From t (Rows [(a1,a2)])) = (a1,a2)
type instance SelectRowType (From t (Rows [(a1,a2,a3)])) = (a1,a2,a3)
type instance SelectRowType (From t (Rows [(a1,a2,a3,a4)])) = (a1,a2,a3,a4)

instance ( HasTable   (From t (Rows [row])) PostgreSQLEngine
         , HasColumns (From t (Rows [row]))
         , FromRow row
         ) => SelectStatement (From t (Rows [row])) IO PostgreSQLEngine
      where
        type SelectResult (From t (Rows [row])) = [row]
        select eng q = do
          conn <- getConnection eng
          query_ conn [qc|select {cols} from {table}|]
          where table = tablename eng q
                cols  = Text.intercalate "," (columns q)

instance ( HasColumn t (Proxy a)
         , HasColumn t (Proxy b)
         , KnownSymbol t
         ) => HasColumns (From t (Rows [(a, b)])) where
  columns = const [ column (Proxy @t) (Proxy @a), column (Proxy @t) (Proxy @b) ]


instance ( HasColumn t (Proxy a1)
         , HasColumn t (Proxy a2)
         , HasColumn t (Proxy a3)
         , KnownSymbol t
         ) => HasColumns (From t (Rows [(a1, a2, a3)])) where

  columns = const [ column (Proxy @t) (Proxy @a1)
                  , column (Proxy @t) (Proxy @a2)
                  , column (Proxy @t) (Proxy @a3)
                  ]


instance ( HasColumn t (Proxy a1)
         , HasColumn t (Proxy a2)
         , HasColumn t (Proxy a3)
         , HasColumn t (Proxy a4)
         , KnownSymbol t
         ) => HasColumns (From t (Rows [(a1, a2, a3, a4)])) where

  columns = const [ column (Proxy @t) (Proxy @a1)
                  , column (Proxy @t) (Proxy @a2)
                  , column (Proxy @t) (Proxy @a3)
                  , column (Proxy @t) (Proxy @a4)
                  ]

instance KnownSymbol t => HasTable (From t cols) e where
  tablename _ _ = fromString (symbolVal (Proxy @t))

instance HasColumn "bondshortname" (Proxy Symbol) where
  column _ _ = "symbol"

instance HasColumn "bondshortname" (Proxy BondShortName) where
  column _ _ = "value"

instance HasColumn "bondfullname" (Proxy Symbol) where
  column _ _ = "symbol"

instance HasColumn "bondfullname" (Proxy BondFullName) where
  column _ _ = "value"

instance HasColumn "bondtradeagg" (Proxy Symbol) where
  column _ _ = "symbol"

instance HasColumn "bondcouponfreq" (Proxy Symbol) where
  column _ _ = "symbol"

instance HasColumn "bondcouponfreq" (Proxy BondCouponFreq) where
  column _ _ = "value"

instance HasColumn "bondissuedate" (Proxy Symbol) where
  column _ _ = "symbol"

instance HasColumn "bondissuedate" (Proxy BondIssueDate) where
  column _ _ = "value"

instance HasColumn "bondmatdate" (Proxy Symbol) where
  column _ _ = "symbol"

instance HasColumn "bondmatdate" (Proxy BondMatDate) where
  column _ _ = "value"

newtype TradeAggDate = TradeAggDate Day
                       deriving (Eq,Ord,Show,Data,Generic)

instance Newtype TradeAggDate Day

newtype TradeAggVol  = TradeAggVol (Maybe Scientific)
                       deriving (Eq,Ord,Show,Data,Generic)

instance Newtype TradeAggVol (Maybe Scientific)

newtype TradeAggNum  = TradeAggNum  Int
                       deriving (Eq,Ord,Show,Data,Generic)

instance Newtype TradeAggNum Int

instance HasColumn "bondtradeagg" (Proxy TradeAggDate) where
  column _ _  = "day"

instance HasColumn "bondtradeagg" (Proxy TradeAggVol) where
  column _ _  = "vol"

instance HasColumn "bondtradeagg" (Proxy TradeAggNum) where
  column _ _  = "trades"

instance HasColumn "vbonddate" (Proxy Symbol) where
  column _ _  = "symbol"

instance HasColumn "vbonddate" (Proxy BondIssueDate) where
  column _ _  = "issuedate"

instance HasColumn "vbonddate" (Proxy BondMatDate) where
  column _ _  = "matdate"

instance HasColumn "vbonddate" (Proxy BondCouponFreq) where
  column _ _  = "freq"

