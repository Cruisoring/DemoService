using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;

namespace DemoService.Core.Converters
{
    public class NumberComparer : IEqualityComparer, IEqualityComparer<IConvertible>
    {
        public const int ConcernedDigits = 8;

        public static NumberStyles[] NumberStyleses = new NumberStyles[]
        {
            NumberStyles.Number | NumberStyles.AllowCurrencySymbol,
            NumberStyles.AllowDecimalPoint | NumberStyles.AllowThousands,
        };
        public static Decimal AsDecimal(IConvertible obj, int digits = ConcernedDigits)
        {
            if (obj is decimal objDecimal)
            {
                return Math.Round(objDecimal, digits);
            }

            string objString = obj.ToString().Trim();

            Decimal parsedValue;

            if (!Decimal.TryParse(objString, out parsedValue)
                && !Decimal.TryParse(objString.Replace(",", "").Replace("$", ""), out parsedValue)
                && !NumberStyleses.Any(numerStyle =>
                    Decimal.TryParse(objString, numerStyle, CultureInfo.InvariantCulture, out parsedValue)))
            {
                throw new ArgumentException($"Cannot parse '{obj}' to Decimal or expected NumberStyle has not been added to {nameof(NumberStyleses)}");
            }


            return Math.Round(parsedValue, digits); ;
        }

        public bool Equals(IConvertible x, IConvertible y)
        {
            decimal xDecimal = AsDecimal(x);
            decimal yDecimal = AsDecimal(y);
            return xDecimal.Equals(yDecimal);
        }

        public int GetHashCode(IConvertible obj)
        {
            decimal d = AsDecimal(obj);
            return d.GetHashCode();
        }

        public bool Equals(object x, object y)
        {
            if (x == null && y == null)
                return true;
            else if (x == null || y == null)
                return false;

            if (x is IConvertible xConvertible && y is IConvertible yConvertible)
            {
                return Equals(xConvertible, yConvertible);
            }
            else
            {
                throw new ArgumentException("Both types of x and y must be IConvertable");
            }
        }

        public int GetHashCode(object obj)
        {
            if (obj == null)
                return 0;
            else if (obj is IConvertible objConvertible)
                return GetHashCode(objConvertible);
            else
            {
                throw new ArgumentException($"Type of {obj.GetType()} is not supported by the {nameof(NumberComparer)}");
            }
        }
    }

}
