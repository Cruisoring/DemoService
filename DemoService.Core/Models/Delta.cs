using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace DemoService.Core.Models
{
    public class Delta : IDelta
    {
        public static bool IsDifferent(object leftObj, object rightObj, IEqualityComparer comparer = null)
        {
            if (comparer == null)
            {
                if (leftObj == null && rightObj == null)
                    return false;
                else if (leftObj == null || rightObj == null)
                    return true;
                return !leftObj.Equals(rightObj);
            }
            else
            {
                return !comparer.Equals(leftObj, rightObj);
            }
        }

        private static void ensureDifferent(object leftObj, object rightObj)
        {
            if (!IsDifferent(leftObj, rightObj))
                throw new ArgumentException();
        }

        public static Delta NewDelta(string leftValueName, string rightValueName, object leftValue, object rightValue)
        {
            ensureDifferent(leftValue, rightValue);
            return new Delta(leftValueName, rightValueName, leftValue, rightValue);
        }

        public static Delta NewDelta(string sharedValueName, object leftValue, object rightValue)
        {
            ensureDifferent(leftValue, rightValue);
            return new Delta(sharedValueName, sharedValueName, leftValue, rightValue);
        }

        public static Delta MissingLeft(string rightValueName)
        {
            //TODO: shall null be treated as Missing?
            return new Delta(null, rightValueName, null, null);
        }

        public static Delta MissingRight(string leftValueName)
        {
            //TODO: shall null be treated as Missing?
            return new Delta(leftValueName, null, null, null);
        }

        public string LeftValueName { get; set; }
        public string RightValueName { get; set; }

        public object LeftValue { get; set; }
        public object RightValue { get; set; }

        private Delta(string leftValueName, string rightValueName, object leftValue, object rightValue)
        {
            this.LeftValueName = leftValueName;
            this.RightValueName = rightValueName;
            this.LeftValue = leftValue;
            this.RightValue = rightValue;
        }

        public override string ToString()
        {
            if (LeftValueName == null)
            {
                return $"Missing Left Value: '{RightValueName}'";
            }
            else if (RightValueName == null)
            {
                return $"Missing Right Value: '{LeftValueName}'";
            }
            else
                return LeftValueName.Equals(RightValueName)
                ? $"'{LeftValueName}': LEFT value '{LeftValue}' of {LeftValue?.GetType().Name} != RIGHT value '{RightValue}' of {RightValue?.GetType().Name}"
                : $"LEFT '{LeftValueName}' value '{LeftValue}' of {LeftValue?.GetType().Name} != '{RightValue}' of {RightValue?.GetType().Name} of '{RightValueName}' RIGHT";
        }

        public bool WithDifference()
        {
            return IsDifferent(LeftValue, RightValue);
        }
    }

}
