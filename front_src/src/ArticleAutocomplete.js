import React from 'react'
import { AutoComplete } from 'antd'

const searchResult = async (api, token, query) => {
    return fetch(`${api}articles?token=${token}&name=${query}`)
        .then((response) => response.json())
        .then((body) =>
            body.result.map((user) => ({
                label: `${user.name}`,
                value: user.id,
            })),
        )
}

class ArticleAutocomplete extends React.Component {
    constructor(props) {
        super(props)

        this.state = {
            options: [],
        }
    }

    async componentDidMount() {
        await this.handleSearch('')
    }

    handleSearch = async (value) => {
        this.setState({
            options: await searchResult(this.props.api, this.props.token, value)
        })
    }

    render() {
        const { options } = this.state

        return (
            <AutoComplete
                {...this.props}
                mode=""
                options={options}
                onSearch={this.handleSearch}
            >
            </AutoComplete>
        )
    }
}

export default ArticleAutocomplete