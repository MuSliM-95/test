import React from "react";

class TextBlock extends React.Component {
    get_text = () => {
        const tax = this.props.tax
        const tax_type = this.props.tax_type
        const amount = Number(this.props.amount).toFixed(2)

        const tax_amount = Number(amount * (tax/100)).toFixed(2)

        if (tax === "0" || tax === 0) {
            return `Сумма операции: ${amount} (без налога)`
        }

        else {
            if (tax_type === "internal") {
                return `Сумма операции: ${amount}, включая налог ${tax_amount}, чистыми ${(amount - tax_amount).toFixed(2)}. (-${tax}% внутри)`
            }
            else {
                const amount_with_tax = ((amount * 100) / (100 - Number(tax))).toFixed(2)
                return `Сумма операции: ${amount_with_tax}, включая налог +${(amount_with_tax - amount).toFixed(2)}, чистыми ${amount}. (+${tax}% поверх)`
            }
        }
    }

    render() {
        return (
            <div className="tax_amount">
                {this.get_text()}
            </div>
        );
    }
}


export default TextBlock